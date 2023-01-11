mod entry;
mod key;
mod lock;
mod lru;
mod tree;
mod tree_data;

pub use tree::Tree;

use {
	crate::{
		resource::Buf, storage, util::trim_zeros_end, Background, BlockSize, Dev, Error,
		MaxRecordSize, Record, Resource, Store,
	},
	alloc::rc::Rc,
	core::{
		cell::{RefCell, RefMut},
		fmt,
		future::{self, Future},
		mem,
		pin::Pin,
		task::{Poll, Waker},
	},
	entry::{Entry, EntryRef},
	futures_util::{stream::FuturesUnordered, Stream, StreamExt, TryStreamExt},
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
// TODO generic... constants?
const CACHE_ENTRY_FIXED_COST: usize = 32; //mem::size_of::<Entry<>>();

/// Cache data.
pub(crate) struct CacheData<R: Resource> {
	/// Cached records of objects and the object list.
	///
	/// The key, in order, is `(id, depth, offset)`.
	/// Using separate hashmaps allows using only a prefix of the key.
	data: FxHashMap<u64, TreeData<R>>,
	/// LRUs to manage cache size.
	lrus: Lrus,
	/// Record entries that are currently being fetched.
	fetching: FxHashMap<Key, Fetching>,
	/// Record entries that are currently being flushed.
	///
	/// Such entries cannot be read from or written to until the flush finishes.
	///
	/// If any wakers have been registered during the flush the entry will not be evicted.
	flushing: FxHashMap<Key, Flushing>,
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

impl<R: Resource> CacheData<R> {
	/// Deallocate a single object ID.
	fn dealloc_id(&mut self, id: u64) {
		debug_assert!(self.used_objects_ids.contains(&id), "double free");
		self.used_objects_ids.remove(id..id + 1);
	}
}

impl<R: Resource + fmt::Debug> fmt::Debug for CacheData<R> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		struct FmtData<'a, R: Resource>(&'a FxHashMap<u64, TreeData<R>>);

		impl<R: Resource + fmt::Debug> fmt::Debug for FmtData<'_, R> {
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
}

impl Lrus {
	/// Create a new *dirty* entry.
	fn create_entry<R: Resource>(&mut self, key: Key, mut data: R::Buf) -> Entry<R> {
		trim_zeros_end(&mut data);
		let global_index = self.global.lru.insert(key);
		self.global.cache_size += data.len() + CACHE_ENTRY_FIXED_COST;
		Entry { data, global_index }
	}

	/// Adjust cache usage based on manually removed entry.
	fn adjust_cache_removed_entry<R: Resource>(&mut self, entry: &Entry<R>) {
		self.global.lru.remove(entry.global_index);
		self.global.cache_size -= entry.data.len() + CACHE_ENTRY_FIXED_COST;
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

/// Entry fetch state.
#[derive(Debug)]
struct Fetching {
	/// Amount of tasks waiting for this entry.
	refcount: usize,
	/// Wakers for tasks waiting on the task that is currently fetching the entry.
	wakers: Vec<Waker>,
	/// The record that must be used if another attempt at fetching is made.
	record: Record,
	/// Whether a task is currently fetching this entry.
	in_progress: bool,
}

/// Entry flush state.
#[derive(Debug)]
struct Flushing {
	/// The entry is currently being flushed and should not be fetched.
	wakers: Vec<Waker>,
	/// Whether the record has been destroyed while flushing it.
	destroyed: bool,
}

/// Cache algorithm.
#[derive(Debug)]
pub(crate) struct Cache<D: Dev, R: Resource> {
	/// The non-volatile backing store.
	store: Store<D, R>,
	/// The cached data.
	data: RefCell<CacheData<R>>,
}

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Initialize a cache layer.
	///
	/// # Panics
	///
	/// If `global_cache_max` is smaller than `dirty_cache_max`.
	///
	/// If `dirty_cache_max` is smaller than the maximum record size.
	pub fn new(store: Store<D, R>, global_cache_max: usize) -> Self {
		assert!(
			global_cache_max >= 1 << store.max_record_size().to_raw(),
			"cache size is smaller than the maximum record size"
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
	async fn get_object_root<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		id: u64,
	) -> Result<Record, Error<D>> {
		trace!("get_object_root {}", id);
		if id == OBJECT_LIST_ID {
			return Ok(self.store.object_list());
		}
		if let Some(root) = self.data.borrow_mut().dangling_roots.get(&id) {
			return Ok(*root);
		}
		let offset = id * (mem::size_of::<Record>() as u64);
		let list = Tree::new_object_list(self, bg).await?;
		let mut root = Record::default();
		let l = list.read(offset, root.as_mut()).await?;
		debug_assert_eq!(l, mem::size_of::<Record>(), "root wasn't fully read");
		Ok(root)
	}

	/// Update an existing root,
	/// i.e. without resizing the object list.
	async fn set_object_root<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		id: u64,
		root: &Record,
	) -> Result<(), Error<D>> {
		if id == OBJECT_LIST_ID {
			self.store.set_object_list(*root);
			return Ok(());
		}
		if let Some(r) = self.data.borrow_mut().dangling_roots.get_mut(&id) {
			*r = *root;
			return Ok(());
		}
		let offset = id * (mem::size_of::<Record>() as u64);
		let list = Tree::new_object_list(self, bg).await?;
		let l = list.write(offset, root.as_ref()).await?;
		debug_assert_eq!(l, mem::size_of::<Record>(), "root wasn't fully written");
		Ok(())
	}

	async fn write_object_table<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		id: u64,
		data: &[u8],
	) -> Result<usize, Error<D>> {
		let offset = id * (mem::size_of::<Record>() as u64);
		let min_len = offset + u64::try_from(data.len()).unwrap();

		let list = Tree::new_object_list(self, bg).await?;

		if min_len > list.len().await? {
			list.resize(min_len).await?;
		}

		list.write(offset, data).await
	}

	/// Create an object.
	pub async fn create<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
	) -> Result<Tree<'a, 'b, D, R>, Error<D>> {
		trace!("create");
		let id = self.create_many::<1>(bg).await?;
		Tree::new(self, bg, id).await
	}

	/// Create many adjacent objects.
	pub async fn create_many<'a, 'b, const N: usize>(
		&'a self,
		bg: &'b Background<'a, D>,
	) -> Result<u64, Error<D>> {
		trace!("create_many {}", N);
		// Allocate
		let id = self.alloc_ids(N.try_into().unwrap());

		// Init
		let mut b = [[0; 32]; N];
		for c in &mut b {
			c.copy_from_slice(Record { references: 1.into(), ..Default::default() }.as_ref());
		}
		self.write_object_table(bg, id, b.flatten()).await?;

		// Tadha!
		Ok(id)
	}

	/// Get an object.
	pub async fn get<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		id: u64,
	) -> Result<Tree<'a, 'b, D, R>, Error<D>> {
		trace!("get {}", id);
		Tree::new(self, bg, id).await
	}

	/// Remove an entry from the cache.
	///
	/// The second value indicates whether the entry is dirty.
	fn remove_entry(&self, key: Key) -> Option<(Entry<R>, bool)> {
		let data = { &mut *self.data.borrow_mut() };
		let (entry, is_dirty) = key.remove_entry(self.max_record_size(), &mut data.data)?;
		data.lrus.adjust_cache_removed_entry(&entry);
		Some((entry, is_dirty))
	}

	/// Move an object to a specific ID.
	///
	/// The old object is destroyed.
	pub async fn move_object<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		from: u64,
		to: u64,
	) -> Result<(), Error<D>> {
		trace!("move_object {} -> {}", from, to);

		if from == to {
			return Ok(()); // Don't even bother.
		}

		// Free allocations in target object.
		self.get(bg, to).await?.resize(0).await?;

		// Move root out of from object.
		let rec = self.get_object_root(bg, from).await?;
		self.data.borrow_mut().dangling_roots.insert(from, rec);
		// Clear original root
		// We can't use set_object_root as it checks dangling_roots
		let offt = mem::size_of::<Record>() as u64 * from;
		Tree::new_object_list(self, bg)
			.await?
			.write_zeros(offt, mem::size_of::<Record>() as _)
			.await?;

		// Fetch entry with to object root now to ensure we can store the entry later with
		// no flushes.
		let _ = self.get_object_root(bg, to).await?;

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
			.modify(bg, |data| {
				let offt = usize::try_from(offset % (1u64 << self.max_record_size())).unwrap();
				let len = data.len().max(offt + mem::size_of::<Record>());
				data.resize(len, 0);
				data.get_mut()[offt..offt + mem::size_of::<Record>()]
					.copy_from_slice(root.as_ref());
			})
			.await?;

		Ok(())
	}

	/// Finish the current transaction, committing any changes to the underlying devices.
	pub async fn finish_transaction<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
	) -> Result<(), Error<D>> {
		// First flush cache
		self.flush_all(bg).await?;

		// Flush store-specific data.
		self.store.finish_transaction().await?;

		Ok(())
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
	) -> Result<EntryRef<'a, D, R>, Error<D>> {
		trace!("fetch_entry {:?} <- {:?}", key, record.lba);

		// Manual sanity check in case entry is already present and Store::read is not called.
		#[cfg(debug_assertions)]
		self.store.assert_alloc(record);

		// Steps:
		//
		// 1. First check if the entry is already present.
		//    If so, just return it.
		// 2. Insert state in CacheData::fetching
		// 3. Try to get the entry.
		// 4. Check if the entry is being flushed.
		//    If so, wait until the flush finishes.
		// 5. Check if the entry is already being fetched.
		//    If so, wait for the fetch to finish.
		//    If not, fetch and return when ready.
		// 6. Repeat from 3.

		// 1. First check if the entry is already present.
		if let Some(entry) = self.get_entry(key) {
			return Ok(entry);
		}

		// 2. Insert state in CacheData::fetching
		let mut data = self.data.borrow_mut();
		match data.fetching.entry(key) {
			hash_map::Entry::Occupied(e) => e.into_mut().refcount += 1,
			hash_map::Entry::Vacant(e) => {
				e.insert(Fetching {
					wakers: vec![],
					refcount: 1,
					record: *record,
					in_progress: false,
				});
			}
		}
		drop(data);

		let insert = move |mut data: RefMut<'a, CacheData<R>>, d: Result<R::Buf, _>| {
			// Check if an error occurred.
			let d = match d {
				Ok(d) => d,
				Err(e) => return Poll::Ready(Err(e)),
			};

			// Remove reference to state
			let hash_map::Entry::Occupied(mut state) = data.fetching.entry(key)
				else { panic!("no fetching entry") };
			state.get_mut().in_progress = false;
			state.get_mut().refcount -= 1;
			if state.get_mut().refcount == 0 {
				debug_assert!(state.get().wakers.is_empty(), "wakers with no references");
				state.remove();
			} else {
				// Wake other tasks waiting for this entry.
				state.get_mut().wakers.drain(..).for_each(|w| w.wake());
			}

			// Insert entry & return it.
			let (trees, mut lrus) = RefMut::map_split(data, |d| (&mut d.data, &mut d.lrus));
			let tree = RefMut::map(trees, |t| {
				t.entry(key.id())
					.or_insert_with(|| TreeData::new(max_depth))
			});
			let levels = RefMut::map(tree, |t| &mut t.data[usize::from(key.depth())]);

			let entry = RefMut::map(levels, |l| {
				let entry: Entry<R> = Entry { data: d, global_index: lrus.global.lru.insert(key) };
				lrus.global.cache_size += entry.data.len() + CACHE_ENTRY_FIXED_COST;
				l.entries
					.try_insert(key.offset(), entry)
					.expect("entry was already present")
			});
			Poll::Ready(Ok(EntryRef::new(self, key, entry, lrus)))
		};

		let read = |record| async move { self.store.read(&record).await };

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

			// 3. Try to get the entry.
			if let Some(entry) = self.get_entry(key) {
				// TODO try to avoid double call to get_entry
				drop(entry);
				// Remove reference to state
				let mut data = self.data.borrow_mut();
				let hash_map::Entry::Occupied(mut state) = data.fetching.entry(key)
					else { panic!("no fetching entry") };
				debug_assert!(
					state.get().wakers.is_empty(),
					"there should be no wakers with entry present"
				);
				state.get_mut().refcount -= 1;
				if state.get_mut().refcount == 0 {
					state.remove();
				}
				drop(data);
				let entry = self.get_entry(key).unwrap();
				return Poll::Ready(Ok(entry));
			}

			// 4. Check if the entry is being flushed.
			let mut data = self.data.borrow_mut();
			if let Some(state) = data.flushing.get_mut(&key) {
				state.wakers.push(cx.waker().clone());
				return Poll::Pending;
			}

			// 5. Check if the entry is already being fetched.
			let state = data.fetching.get_mut(&key).expect("no fetching entry");
			if state.in_progress {
				state.wakers.push(cx.waker().clone());
				Poll::Pending
			} else {
				state.in_progress = true;
				// Fetch record
				let f = fetching.insert(read(state.record));
				drop(data);
				// SAFETY: we won't move the future in fetching to anywhere else.
				let f = unsafe { Pin::new_unchecked(f) };
				let Poll::Ready(d) = Future::poll(f, cx) else { return Poll::Pending };
				// Insert entry & return.
				insert(self.data.borrow_mut(), d)
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
		if let Some((entry, _)) = key.remove_entry(self.max_record_size(), &mut data.data) {
			data.lrus.adjust_cache_removed_entry(&entry);
		}
		// If the record was already being flushed/evicted in the background, mark it as
		// destroyed.
		if let Some(state) = data.flushing.get_mut(&key) {
			dbg!();
			state.destroyed = true;
		}
	}

	/// Get a mutable reference to an object's data.
	///
	/// # Panics
	///
	/// If another borrow is alive.
	fn get_object_entry_mut(&self, id: u64, max_depth: u8) -> RefMut<'_, TreeData<R>> {
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
	pub async fn resize_cache<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		global_max: usize,
	) -> Result<(), Error<D>> {
		{
			let mut data = { &mut *self.data.borrow_mut() };
			data.lrus.global.cache_max = global_max;
		}
		self.flush(bg).await
	}

	/// Flush if total memory use exceeds limits.
	async fn flush<'a, 'b>(&'a self, bg: &'b Background<'a, D>) -> Result<(), Error<D>> {
		let mut data = self.data.borrow_mut();

		if data.is_flushing {
			// Don't bother.
			return Ok(());
		}

		data.is_flushing = true;
		drop(data);

		trace!("flush");
		self.evict_excess(bg).await?;

		self.data.borrow_mut().is_flushing = false;
		Ok(())
	}

	/// Evict entries if global cache limits are being exceeded.
	async fn evict_excess<'a, 'b>(&'a self, bg: &'b Background<'a, D>) -> Result<(), Error<D>> {
		trace!("evict_excess");
		let mut data = self.data.borrow_mut();

		while data.lrus.global.cache_size > data.lrus.global.cache_max {
			// Get last read entry.
			let &key = data
				.lrus
				.global
				.lru
				.last()
				.expect("no nodes despite non-zero write cache size");

			// Push to background tasks queue to process in parallel
			drop(data);
			if let Some(task) = self.evict_entry(key) {
				bg.run_background(Box::pin(task));
			}
			data = self.data.borrow_mut();
		}

		Ok(())
	}

	/// Evict an entry from the cache.
	///
	/// Does nothing if the entry wasn't present.
	fn evict_entry(&self, key: Key) -> Option<impl Future<Output = Result<(), Error<D>>> + '_> {
		trace!("evict_entry {:?}", key);
		let (entry, is_dirty) = self.remove_entry(key)?;

		// Temporarily block fetches.
		// Blocking fetches is not ideal but it's easy.
		// Given that the entry was at the end of the queue it is unlikely it will be
		// needed soon anyways.
		if is_dirty {
			let _r = self
				.data
				.borrow_mut()
				.flushing
				.insert(key, Flushing { wakers: vec![], destroyed: false });
			debug_assert!(_r.is_none(), "entry was already being flushed");
		}

		is_dirty.then(|| async move {
			trace!("evict_entry::is_dirty {:?}", key);
			// Store record.
			let record = self.store.write(entry.data).await?;

			// Check if the corresponding record changed in the meantime.
			//
			// This may happen due to destroy(), which is a non-async function.
			//
			// If so, destroy the newly created entry and return immediately.
			let mut data = self.data.borrow_mut();
			let hash_map::Entry::Occupied(state) = data.flushing.entry(key)
				else { panic!("entry not marked as being flushed") };
			if dbg!(state.get().destroyed) {
				self.store.destroy(&record);
				state.remove().wakers.drain(..).for_each(|w| w.wake());
				// Set record if any records are attempting to fetch this entry.
				if let Some(state) = data.fetching.get_mut(&key) {
					todo!();
					// TODO Waking just one task for fetching should be sufficient.
					state.wakers.drain(..).for_each(|w| w.wake());
				}
				return Ok(());
			}
			drop(data);

			let bg = Default::default(); // TODO get rid of this sillyness
			let updated = Tree::new(self, &bg, key.id())
				.await?
				.update_record(key.depth(), key.offset(), record)
				.await?;
			// Unmark as being flushed.
			let mut data = self.data.borrow_mut();
			// Wake waiting fetchers.
			data.flushing
				.remove(&key)
				.expect("entry not marked as being flushed")
				.wakers
				.drain(..)
				.for_each(|w| w.wake());
			// Set record if any records are attempting to fetch this entry.
			if let Some(state) = data.fetching.get_mut(&key) {
				state.record = record;
				// TODO Waking just one task for fetching should be sufficient.
				state.wakers.drain(..).for_each(|w| w.wake());
			}
			drop(data);
			// Make sure we only drop the "background" runner at the end to avoid
			// getting stuck when something tries to fetch the entry that is
			// being evicted.
			//
			// Specifically, we must ensure the Flushing state is removed before
			// we attempt to run the background tasks to completion.
			bg.drop().await
		})
	}

	/// Flush an entry from the cache.
	///
	/// This does not evict the entry.
	///
	/// Does nothing if the entry wasn't present or dirty.
	async fn flush_entry<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		key: Key,
	) -> Result<(), Error<D>> {
		trace!("flush_entry {:?}", key);
		let mut data = self.data.borrow_mut();
		let Some(tree) = data.data.get_mut(&key.id()) else { return Ok(()) };
		let [level, levels @ ..] = &mut tree.data[usize::from(key.depth())..] else {
			panic!("depth out of range")
		};

		// FIXME clear dirty status
		let Some(counter) = level.dirty_counters.get_mut(&key.offset()) else { return Ok(()) };
		*counter -= 1;
		*counter &= isize::MAX;
		if *counter == 0 {
			level.dirty_counters.remove(&key.offset());
		}

		// Propagate up
		let mut offt = key.offset() >> self.entries_per_parent_p2();
		for lvl in levels {
			let hash_map::Entry::Occupied(mut e) = lvl.dirty_counters.entry(offt)
				else { panic!("missing dirty counter in ancestor") };
			*e.get_mut() -= 1;
			debug_assert_ne!(*e.get(), isize::MIN, "dirty without references");
			if *e.get() == 0 {
				e.remove();
			}
			offt >>= self.entries_per_parent_p2();
		}

		let entry = level
			.entries
			.get_mut(&key.offset())
			.expect("dirty entry not present");

		// FIXME Temporarily block other fetches & flushes, in case the entry gets evicted during await.

		// Store record.
		let entry_data = entry.data.clone();
		drop(data);
		let rec = self.store.write(entry_data).await?;
		Tree::new(self, bg, key.id())
			.await?
			.update_record(key.depth(), key.offset(), rec)
			.await?;

		Ok(())
	}

	/// Flush all entries.
	async fn flush_all<'a, 'b>(&'a self, bg: &'b Background<'a, D>) -> Result<(), Error<D>> {
		trace!("flush_all");
		// Go through all trees and flush from bottom to top.
		//
		// Start from the bottom of all trees since those are trivial to all flush in parallel.

		// Wait for all "background" tasks to finish, which may include active flushes.
		bg.try_run_all().await?;

		let mut data = self.data.borrow_mut();
		let mut queue = FuturesUnordered::new();

		// Flush all objects except the object list,
		// since the latter will get a lot of updates to the leaves.
		let ids = data
			.data
			.keys()
			.copied()
			.filter(|&id| id != OBJECT_LIST_ID)
			.collect::<Vec<_>>();
		for depth in 0..16 {
			for &id in ids.iter() {
				let Some(tree) = data.data.get_mut(&id) else { continue };
				let Some(level) = tree.data.get_mut(usize::from(depth)) else { continue };
				let offsets = level.dirty_counters.keys().copied().collect::<Vec<_>>();
				drop(data);

				// Flush in parallel.
				for offt in offsets {
					let key = Key::new(id, depth, offt);
					queue.push(self.flush_entry(bg, key));
				}
				while queue.try_next().await?.is_some() {}

				// Wait for background tasks in case higher records got flushed.
				bg.try_run_all().await?;

				data = self.data.borrow_mut();
			}
		}

		// Now flush the object list.
		for depth in 0..16 {
			let Some(tree) = data.data.get_mut(&OBJECT_LIST_ID) else { continue };
			let Some(level) = tree.data.get_mut(usize::from(depth)) else { continue };
			let offsets = level.dirty_counters.keys().copied().collect::<Vec<_>>();
			drop(data);

			// Flush in parallel.
			for offt in offsets {
				let key = Key::new(OBJECT_LIST_ID, depth, offt);
				queue.push(self.flush_entry(bg, key));
			}
			while queue.try_next().await?.is_some() {}

			// Wait for background tasks in case higher records got flushed.
			bg.try_run_all().await?;

			data = self.data.borrow_mut();
		}

		// Tadha!
		// Do a sanity check just in case.
		if cfg!(debug_assertions) {
			for tree in data.data.values() {
				for level in tree.data.iter() {
					debug_assert!(
						level.dirty_counters.is_empty(),
						"flush_all didn't flush all"
					);
				}
			}
		}
		Ok(())
	}

	/// Unmount the cache.
	///
	/// The cache is flushed before returning the underlying [`Store`].
	pub async fn unmount(self) -> Result<Store<D, R>, Error<D>> {
		trace!("unmount");
		let bg = Default::default();
		self.finish_transaction(&bg).await?;
		bg.drop().await?;
		Ok(self.store)
	}

	/// Get statistics for this sesion.
	pub fn statistics(&self) -> Statistics {
		#[cfg(test)]
		self.verify_cache_usage();

		let data = self.data.borrow();
		Statistics { storage: self.store.statistics(), global_usage: data.lrus.global.cache_size }
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

	fn resource(&self) -> &R {
		self.store.resource()
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
}
