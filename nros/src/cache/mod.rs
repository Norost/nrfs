mod entry;
mod key;
mod lock;
mod lru;
mod tree;
mod tree_data;

pub use tree::Tree;

use {
	crate::{storage, BlockSize, Dev, Error, MaxRecordSize, Record, Store},
	core::{
		cell::{RefCell, RefMut},
		fmt,
		future::{self, Future},
		mem,
		pin::Pin,
		task::{Poll, Waker},
	},
	entry::{Entry, EntryRef},
	key::Key,
	lock::ResizeLock,
	rangemap::RangeSet,
	rustc_hash::FxHashMap,
	std::collections::hash_map,
	tree_data::{FmtTreeData, TreeData},
};

/// Fixed ID for the object list so it can use the same caching mechanisms as regular objects.
const OBJECT_LIST_ID: u64 = 1 << 59; // 2**64 / 2**5 = 2**59, ergo 2**59 is just out of range.

/// Record size as a power-of-two.
const RECORD_SIZE_P2: u8 = mem::size_of::<Record>().ilog2() as _;

/// Estimated fixed cost for every cached entry.
///
/// This is in addition to the amount of data stored by the entry.
const CACHE_ENTRY_FIXED_COST: usize = mem::size_of::<Entry>();

/// Cache data.
pub(crate) struct CacheData {
	/// Cached records of objects and the object list.
	///
	/// The key, in order, is `(id, depth, offset)`.
	/// Using separate hashmaps allows using only a prefix of the key.
	data: FxHashMap<u64, TreeData>,
	/// LRUs to manage cache size.
	lrus: Lrus,
	/// Record entries that are currently being fetched.
	fetching: FxHashMap<Key, Vec<Waker>>,
	/// Record entries that are currently being flushed.
	///
	/// Such entries cannot be read from or written to until the flush finishes.
	///
	/// If any wakers have been registered during the flush the entry will not be evicted.
	flushing: FxHashMap<Key, Vec<Waker>>,
	/// Trees that are being resized.
	resizing: FxHashMap<u64, ResizeLock>,
	/// Dangling roots,
	/// i.e. roots of objects that are being moved by [`Cache::move_object`].
	dangling_roots: FxHashMap<u64, Record>,
	/// Used object IDs.
	used_objects_ids: RangeSet<u64>,
	/// Whether we're already flushing.
	///
	/// If yes, avoid flushing again since that breaks stuff.
	is_flushing: bool,
}

impl CacheData {
	/// Deallocate a single object ID.
	fn dealloc_id(&mut self, id: u64) {
		debug_assert!(self.used_objects_ids.contains(&id), "double free");
		self.used_objects_ids.remove(id..id + 1);
	}
}

impl fmt::Debug for CacheData {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		struct FmtData<'a>(&'a FxHashMap<u64, TreeData>);

		impl fmt::Debug for FmtData<'_> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				let mut f = f.debug_map();
				for (&id, data) in self.0.iter() {
					f.entry(&id, &FmtTreeData { data, id });
				}
				f.finish()
			}
		}

		f.debug_struct(stringify!(CacheData))
			.field("data", &FmtData(&self.data))
			.field("lrus", &self.lrus)
			.field("fetching", &self.fetching)
			.field("flushing", &self.flushing)
			.field("resizing", &self.resizing)
			.field("dangling_roots", &self.dangling_roots)
			.field("used_objects_ids", &self.used_objects_ids)
			.field("is_flushing", &self.is_flushing)
			.finish()
	}
}

/// Cache LRU queues, with tracking per byte used.
#[derive(Debug)]
struct Lrus {
	/// LRU list for global evictions.
	global: Lru,
	/// LRU list for flushing dirty records.
	///
	/// This is not used for eviction but ensures an excessive amount of writes does not hold up
	/// reads.
	dirty: Lru,
}

impl Lrus {
	/// Adjust cache usage based on manually removed entry.
	fn adjust_cache_removed_entry(&mut self, entry: &Entry) {
		self.global.lru.remove(entry.global_index);
		self.global.cache_size -= entry.data.len() + CACHE_ENTRY_FIXED_COST;
		// The entry *must* be dirty as otherwise it either:
		// - wouldn't exist
		// - have a parent node, in which case it was already destroyed in a previous
		//   iteration.
		// ... except if d == cur_depth. Meh
		if let Some(idx) = entry.write_index {
			//debug_assert_eq!(d, cur_depth, "not in dirty LRU");
			self.dirty.lru.remove(idx);
			self.dirty.cache_size -= entry.data.len() + CACHE_ENTRY_FIXED_COST;
		}
	}
}

/// Cache LRU queue, with tracking per byte used.
#[derive(Debug)]
struct Lru {
	/// Linked list for LRU entries
	lru: lru::LruList<Key>,
	/// The maximum amount of total bytes to keep cached.
	cache_max: usize,
	/// The amount of cached bytes.
	cache_size: usize,
}

/// Cache algorithm.
#[derive(Debug)]
pub(crate) struct Cache<D: Dev> {
	/// The non-volatile backing store.
	store: Store<D>,
	/// The cached data.
	data: RefCell<CacheData>,
}

impl<D: Dev> Cache<D> {
	/// Initialize a cache layer.
	///
	/// # Panics
	///
	/// If `global_cache_max` is smaller than `dirty_cache_max`.
	///
	/// If `dirty_cache_max` is smaller than the maximum record size.
	pub fn new(store: Store<D>, global_cache_max: usize, dirty_cache_max: usize) -> Self {
		assert!(
			global_cache_max >= dirty_cache_max,
			"global cache size is smaller than write cache"
		);
		assert!(
			dirty_cache_max >= 1 << store.max_record_size().to_raw(),
			"write cache size is smaller than the maximum record size"
		);

		// TODO iterate over object list to find free slots.
		let mut used_objects_ids = RangeSet::new();
		let len = u64::from(store.object_list().total_length);
		if len > 0 {
			let rec_size = u64::try_from(mem::size_of::<Record>()).unwrap();
			assert_eq!(
				len % rec_size,
				0,
				"todo: total length not a multiple of record size"
			);
			used_objects_ids.insert(0..len / rec_size);
		}

		Self {
			store,
			data: RefCell::new(CacheData {
				data: Default::default(),
				lrus: Lrus {
					global: Lru {
						lru: Default::default(),
						cache_max: global_cache_max,
						cache_size: 0,
					},
					dirty: Lru {
						lru: Default::default(),
						cache_max: dirty_cache_max,
						cache_size: 0,
					},
				},
				fetching: Default::default(),
				flushing: Default::default(),
				resizing: Default::default(),
				dangling_roots: Default::default(),
				used_objects_ids,
				is_flushing: false,
			}),
		}
	}

	/// Allocate an arbitrary amount of object IDs.
	fn alloc_ids(&self, count: u64) -> u64 {
		let mut slf = self.data.borrow_mut();
		for r in slf.used_objects_ids.gaps(&(0..u64::MAX)) {
			if r.end - r.start >= count {
				slf.used_objects_ids.insert(r.start..r.start + count);
				return r.start;
			}
		}
		unreachable!("more than 2**64 objects allocated");
	}

	/// Get an existing root,
	async fn get_object_root(&self, id: u64) -> Result<Record, Error<D>> {
		if id == OBJECT_LIST_ID {
			return Ok(self.store.object_list());
		}
		if let Some(root) = self.data.borrow_mut().dangling_roots.get(&id) {
			return Ok(*root);
		}
		let offset = id * (mem::size_of::<Record>() as u64);
		let list = Tree::new_object_list(self).await?;
		let mut root = Record::default();
		let l = list.read(offset, root.as_mut()).await?;
		debug_assert_eq!(l, mem::size_of::<Record>(), "root wasn't fully read");
		Ok(root)
	}

	/// Update an existing root,
	/// i.e. without resizing the object list.
	async fn set_object_root(&self, id: u64, root: &Record) -> Result<(), Error<D>> {
		if id == OBJECT_LIST_ID {
			self.store.set_object_list(*root);
			return Ok(());
		}
		if let Some(r) = self.data.borrow_mut().dangling_roots.get_mut(&id) {
			*r = *root;
			return Ok(());
		}
		let offset = id * (mem::size_of::<Record>() as u64);
		let list = Tree::new_object_list(self).await?;
		let l = list.write(offset, root.as_ref()).await?;
		debug_assert_eq!(l, mem::size_of::<Record>(), "root wasn't fully written");
		Ok(())
	}

	async fn write_object_table(&self, id: u64, data: &[u8]) -> Result<usize, Error<D>> {
		let offset = id * (mem::size_of::<Record>() as u64);
		let min_len = offset + u64::try_from(data.len()).unwrap();

		let list = Tree::new_object_list(self).await?;

		if min_len > list.len().await? {
			list.resize(min_len).await?;
		}

		list.write(offset, data).await
	}

	/// Create an object.
	pub async fn create(&self) -> Result<Tree<D>, Error<D>> {
		trace!("create");
		let id = self.create_many::<1>().await?;
		Tree::new(self, id).await
	}

	/// Create many adjacent objects.
	pub async fn create_many<const N: usize>(&self) -> Result<u64, Error<D>> {
		trace!("create_many {}", N);
		// Allocate
		let id = self.alloc_ids(N.try_into().unwrap());

		// Init
		let mut b = [[0; 32]; N];
		for c in &mut b {
			c.copy_from_slice(Record { references: 1.into(), ..Default::default() }.as_ref());
		}
		self.write_object_table(id, b.flatten()).await?;

		// Tadha!
		Ok(id)
	}

	/// Get an object.
	pub async fn get(&self, id: u64) -> Result<Tree<D>, Error<D>> {
		trace!("get {}", id);
		Tree::new(self, id).await
	}

	/// Remove an entry from the cache.
	///
	/// This does *not* flush the entry if it is dirty!
	fn remove_entry(&self, key: Key) -> Option<Entry> {
		let data = { &mut *self.data.borrow_mut() };
		let entry = key.remove_entry(self.max_record_size(), &mut data.data)?;
		data.lrus.adjust_cache_removed_entry(&entry);
		Some(entry)
	}

	/// Move an object to a specific ID.
	///
	/// The old object is destroyed.
	pub async fn move_object(&self, from: u64, to: u64) -> Result<(), Error<D>> {
		trace!("move_object {} -> {}", from, to);

		if from == to {
			return Ok(()); // Don't even bother.
		}

		// Free allocations in target object.
		self.get(to).await?.resize(0).await?;

		// Move root out of from object.
		let rec = self.get_object_root(from).await?;
		self.data.borrow_mut().dangling_roots.insert(from, rec);
		// Clear original root
		// We can't use set_object_root as it checks dangling_roots
		let offt = mem::size_of::<Record>() as u64 * from;
		Tree::new_object_list(self)
			.await?
			.write_zeros(offt, mem::size_of::<Record>() as _)
			.await?;

		// Fetch entry with to object root now to ensure we can store the entry later with
		// no flushes.
		let _ = self.get_object_root(to).await?;

		// Move object data & fix LRU entries.
		let mut data = self.data.borrow_mut();
		if let Some(obj) = data.data.remove(&from) {
			for level in obj.data.iter() {
				for entry in level.entries.values() {
					let key = data
						.lrus
						.global
						.lru
						.get_mut(entry.global_index)
						.expect("invalid global LRU index");
					*key = Key::new(to, key.depth(), key.offset());
					if let Some(idx) = entry.write_index {
						let key = data
							.lrus
							.dirty
							.lru
							.get_mut(idx)
							.expect("invalid write LRU index");
						*key = Key::new(to, key.depth(), key.offset());
					}
				}
			}
			data.data.insert(to, obj);
		} else {
			data.data.remove(&to);
		}

		data.dealloc_id(from);

		let root = data
			.dangling_roots
			.remove(&from)
			.expect("from root not dangling");

		// To ensure correctness, manually get the entry that should have already been fetched.
		let offset = mem::size_of::<Record>() as u64 * to;
		let key = Key::new(OBJECT_LIST_ID, 0, offset >> self.max_record_size());
		let (data, lrus) = RefMut::map_split(data, |d| (&mut d.data, &mut d.lrus));
		let entry = RefMut::map(data, |d| {
			d.get_mut(&key.id()).expect("no tree").data[usize::from(key.depth())]
				.entries
				.get_mut(&key.offset())
				.expect("no entry")
		});

		EntryRef::new(self, key, entry, lrus)
			.modify(|data| {
				let offt = usize::try_from(offset % (1u64 << self.max_record_size())).unwrap();
				let len = data.len().max(offt + mem::size_of::<Record>());
				data.resize(len, 0);
				data[offt..offt + mem::size_of::<Record>()].copy_from_slice(root.as_ref());
			})
			.await?;

		Ok(())
	}

	/// Finish the current transaction, committing any changes to the underlying devices.
	pub async fn finish_transaction(&self) -> Result<(), Error<D>> {
		// First flush cache
		let data = self.data.borrow();
		let global_max = data.lrus.global.cache_max;
		let dirty_max = data.lrus.dirty.cache_max;
		drop(data);
		self.resize_cache(global_max, 0).await?;
		debug_assert_eq!(
			self.statistics().dirty_usage,
			0,
			"not all data has been flushed"
		);

		// Flush store-specific data.
		self.store.finish_transaction().await?;

		// Restore cache params
		self.resize_cache(global_max, dirty_max).await
	}

	/// The block size used by the underlying [`Store`].
	pub fn block_size(&self) -> BlockSize {
		self.store.block_size()
	}

	/// The maximum record size used by the underlying [`Store`].
	pub fn max_record_size(&self) -> MaxRecordSize {
		self.store.max_record_size()
	}

	/// Fetch a record for a cache entry.
	///
	/// If the entry is already being fetched,
	/// the caller is instead added to a list to be waken up when the fetcher has finished.
	async fn fetch_entry<'a>(
		&'a self,
		key: Key,
		record: &Record,
		max_depth: u8,
	) -> Result<EntryRef<'a, D>, Error<D>> {
		trace!("fetch_entry {:?} <- {:?}", key, record.lba);

		// Manual sanity check in case entry is already present and Store::read is not called.
		#[cfg(debug_assertions)]
		self.store.assert_alloc(record);

		// We take 4 steps:
		//
		// 1. First check if the entry is already present.
		//    If so, just return it.
		// 2. Check if the entry is being flushed.
		//    If so, wait until the flush finishes.
		// 3. Check if the entry is already being fetched.
		//    If so, wait for the fetch to finish.
		//    If not, fetch and return when ready.
		// 4. Try to get the entry.
		// 5. Repeat from 2.

		let insert = move |mut data: RefMut<'a, CacheData>, d| {
			// Wake other tasks waiting for this entry.
			data.fetching
				.remove(&key)
				.expect("no wakers list")
				.into_iter()
				.for_each(|w| w.wake());

			// Check if an error occurred.
			let d = match d {
				Ok(d) => d,
				Err(e) => return Poll::Ready(Err(e)),
			};

			// Insert entry & return it.
			let (trees, mut lrus) = RefMut::map_split(data, |d| (&mut d.data, &mut d.lrus));
			let tree = RefMut::map(trees, |t| {
				t.entry(key.id())
					.or_insert_with(|| TreeData::new(max_depth))
			});
			let levels = RefMut::map(tree, |t| &mut t.data[usize::from(key.depth())]);

			let entry = RefMut::map(levels, |l| {
				let entry =
					Entry { data: d, global_index: lrus.global.lru.insert(key), write_index: None };
				lrus.global.cache_size += entry.data.len() + CACHE_ENTRY_FIXED_COST;
				l.entries
					.try_insert(key.offset(), entry)
					.expect("entry was already present")
			});
			Poll::Ready(Ok(EntryRef::new(self, key, entry, lrus)))
		};

		let mut fetching = None;

		future::poll_fn(move |cx| {
			// If we're already fetching, poll
			if let Some(f) = &mut fetching {
				// SAFETY: we won't move the future in fetching to anywhere else.
				let f = unsafe { Pin::new_unchecked(f) };
				let Poll::Ready(d) = Future::poll(f, cx) else { return Poll::Pending };
				fetching = None; // We're done with it, avoid polling it again.

				// Insert entry & return.
				return insert(self.data.borrow_mut(), d);
			}

			// 1. First check if the entry is already present.
			if let Some(entry) = self.get_entry(key) {
				return Poll::Ready(Ok(entry));
			}
			let mut data = self.data.borrow_mut();

			// 2. Check if the entry is being flushed.
			if let Some(wakers) = data.flushing.get_mut(&key) {
				wakers.push(cx.waker().clone());
				return Poll::Pending;
			}

			// 3. Check if the entry is already being fetched.
			match data.fetching.entry(key) {
				hash_map::Entry::Vacant(e) => {
					// Add list for other fetchers to register wakers to.
					e.insert(Default::default());
					drop(data);

					// Fetch record
					let f = fetching.insert(self.store.read(record));
					// SAFETY: we won't move the future in fetching to anywhere else.
					let f = unsafe { Pin::new_unchecked(f) };
					let Poll::Ready(d) = Future::poll(f, cx) else { return Poll::Pending };

					// Insert entry & return.
					insert(self.data.borrow_mut(), d)
				}
				hash_map::Entry::Occupied(e) => {
					// Wait until the fetcher for this record finishes.
					e.into_mut().push(cx.waker().clone());
					Poll::Pending
				}
			}
		})
		.await
	}

	/// Fetch an entry and immediately destroy it.
	///
	/// # Note
	///
	/// Nothing may attempt to fetch this entry during or after this call!
	fn destroy_entry(&self, key: Key, record: &Record) {
		trace!("destroy_entry {:?}", key);
		// Check if the entry is present.
		// If yes, remove it.
		// If not, don't bother fetching it as that is a waste of time.
		self.store.destroy(record);
		let mut data = self.data.borrow_mut();
		if let Some(entry) = key.remove_entry(self.max_record_size(), &mut data.data) {
			data.lrus.adjust_cache_removed_entry(&entry);
		}
	}

	/// Get a mutable reference to an object's data.
	///
	/// # Panics
	///
	/// If another borrow is alive.
	fn get_object_entry_mut(&self, id: u64, max_depth: u8) -> RefMut<TreeData> {
		RefMut::map(self.data.borrow_mut(), |data| {
			data.data
				.entry(id)
				.or_insert_with(|| TreeData::new(max_depth))
		})
	}

	/// Get the root record of the object list.
	fn object_list(&self) -> Record {
		self.store.object_list()
	}

	/// Readjust cache size.
	///
	/// This may be useful to increase or decrease depending on total system memory usage.
	///
	/// # Panics
	///
	/// If `global_max < write_max`.
	pub async fn resize_cache(&self, global_max: usize, write_max: usize) -> Result<(), Error<D>> {
		assert!(
			global_max >= write_max,
			"global cache is smaller than write cache"
		);
		{
			let mut data = { &mut *self.data.borrow_mut() };
			data.lrus.global.cache_max = global_max;
			data.lrus.dirty.cache_max = write_max;
		}
		self.flush().await
	}

	/// Flush if total memory use exceeds limits.
	async fn flush(&self) -> Result<(), Error<D>> {
		let mut data = self.data.borrow_mut();

		if data.is_flushing {
			// Don't bother.
			return Ok(());
		}
		data.is_flushing = true;

		trace!("flush");

		while data.lrus.dirty.cache_size > data.lrus.dirty.cache_max {
			// Remove last written to entry.
			let &key = data
				.lrus
				.dirty
				.lru
				.last()
				.expect("no nodes despite non-zero write cache size");

			// Prevent record from being modified while flushing.
			// This also briefly prevents reads but w/e.
			let _r = data.flushing.insert(key, Default::default());
			debug_assert!(_r.is_none(), "entry was already marked as being flushed.");

			// Take data out of entry temporarily.
			let (mut entry, mut lrus) = RefMut::map_split(data, |data| {
				let entry = key.get_entry_mut(&mut data.data).expect("no entry");
				(entry, &mut data.lrus)
			});

			// Remove from write LRU
			lrus.dirty.lru.remove_last().unwrap();
			entry.write_index = None;
			lrus.dirty.cache_size -= entry.data.len() + CACHE_ENTRY_FIXED_COST;
			drop(lrus);

			// Store record.
			// TODO try to do concurrent writes.
			let rec = self.store.write(&entry.data).await?;
			drop(entry); // FIXME RefMut across await point!

			Tree::new(self, key.id())
				.await?
				.update_record(key.depth(), key.offset(), rec)
				.await?;
			data = self.data.borrow_mut();

			// Update dirty counters
			let mut offt = key.offset();
			for level in data.data.get_mut(&key.id()).unwrap().data[key.depth().into()..].iter_mut()
			{
				let std::collections::hash_map::Entry::Occupied(mut c) = level.dirty_counters.entry(offt) else { panic!() };
				*c.get_mut() -= 1;
				if *c.get() == 0 {
					c.remove();
				}
				offt >>= self.entries_per_parent_p2();
			}

			// Wake tasks that are waiting for this entry.
			let wakers = data
				.flushing
				.remove(&key)
				.expect("entry not marked as being flushed");
			for w in wakers {
				w.wake();
			}
		}

		while data.lrus.global.cache_size > data.lrus.global.cache_max {
			// Remove last written to entry.
			let &key = data
				.lrus
				.global
				.lru
				.last()
				.expect("no nodes despite non-zero write cache size");

			drop(data);
			let entry = self.remove_entry(key).expect("entry not present");

			if entry.write_index.is_some() {
				// Store record.
				// TODO try to do concurrent writes.
				let rec = self.store.write(&entry.data).await?;

				Tree::new(self, key.id())
					.await?
					.update_record(key.depth(), key.offset(), rec)
					.await?;
			}
			data = self.data.borrow_mut();
		}

		data.is_flushing = false;
		Ok(())
	}

	/// Evict an entry from the cache.
	///
	/// Does nothing if the entry wasn't present.
	async fn evict_entry(&self, key: Key) -> Result<(), Error<D>> {
		trace!("evict_entry {:?}", key);
		let Some(entry) = self.remove_entry(key) else { return Ok(()) };

		if entry.write_index.is_some() {
			// Store record.
			let rec = self.store.write(&entry.data).await?;
			Tree::new(self, key.id())
				.await?
				.update_record(key.depth(), key.offset(), rec)
				.await?;
		}

		Ok(())
	}

	/// Unmount the cache.
	///
	/// The cache is flushed before returning the underlying [`Store`].
	pub async fn unmount(self) -> Result<Store<D>, Error<D>> {
		trace!("unmount");
		self.finish_transaction().await?;
		Ok(self.store)
	}

	/// Get statistics for this sesion.
	pub fn statistics(&self) -> Statistics {
		#[cfg(test)]
		self.verify_cache_usage();

		let data = self.data.borrow();
		Statistics {
			storage: self.store.statistics(),
			global_usage: data.lrus.global.cache_size,
			dirty_usage: data.lrus.dirty.cache_size,
		}
	}

	/// Check if cache size matches real usage
	#[cfg(test)]
	#[track_caller]
	pub fn verify_cache_usage(&self) {
		let data = self.data.borrow();
		let real_global_usage = data
			.data
			.values()
			.flat_map(|o| o.data.iter())
			.flat_map(|m| m.entries.values())
			.map(|v| v.data.len() + CACHE_ENTRY_FIXED_COST)
			.sum::<usize>();
		assert_eq!(
			real_global_usage, data.lrus.global.cache_size,
			"global cache size mismatch"
		);
	}

	/// Amount of entries in a parent record as a power of two.
	fn entries_per_parent_p2(&self) -> u8 {
		self.max_record_size().to_raw() - RECORD_SIZE_P2
	}
}

/// Statistics for this session.
///
/// Used for debugging.
#[derive(Clone, Copy, Debug, Default)]
pub struct Statistics {
	/// Storage statistics.
	pub storage: storage::Statistics,
	/// Total amount of memory used by record data, including dirty data.
	pub global_usage: usize,
	/// Total amount of memory used by dirty record data.
	pub dirty_usage: usize,
}
