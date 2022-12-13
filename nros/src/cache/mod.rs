mod lru;
mod tree;
mod tree_data;

pub use tree::Tree;

use {
	crate::{util::trim_zeros_end, BlockSize, Dev, Error, MaxRecordSize, Record, Store},
	core::{
		cell::{Ref, RefCell, RefMut},
		fmt,
		future::{self, Future},
		mem,
		ops::{Deref, DerefMut},
		task::{Poll, Waker},
	},
	rangemap::RangeSet,
	rustc_hash::FxHashMap,
	std::collections::hash_map,
	tree_data::{FlushLock, FmtTreeData, TreeData},
};

/// Fixed ID for the object list so it can use the same caching mechanisms as regular objects.
const OBJECT_LIST_ID: u64 = u64::MAX;

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
	/// Object entries that are currently in use and must not be evicted.
	locked_objects: FxHashMap<u64, usize>,
	/// Record entries that are currently in use and must not be evicted.
	locked_records: FxHashMap<(u64, u8, u64), usize>,
	/// Record entries that are currently being fetched.
	fetching: FxHashMap<(u64, u8, u64), Vec<Waker>>,
	/// Used object IDs.
	used_objects_ids: RangeSet<u64>,
	/// Whether we're already flushing.
	///
	/// If yes, avoid flushing again since that breaks stuff.
	is_flushing: bool,
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
			.field("locked_objects", &self.locked_objects)
			.field("locked_records", &self.locked_records)
			.field("fetching", &self.fetching)
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
	lru: lru::LruList<(u64, u8, u64)>,
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
				locked_objects: Default::default(),
				locked_records: Default::default(),
				fetching: Default::default(),
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

	/// Deallocate a single object ID.
	fn dealloc_id(&self, id: u64) {
		let mut slf = self.data.borrow_mut();
		debug_assert!(slf.used_objects_ids.contains(&id), "double free");
		slf.used_objects_ids.remove(id..id + 1);
	}

	/// Get an existing root,
	async fn get_object_root(&self, id: u64) -> Result<Record, Error<D>> {
		if id == OBJECT_LIST_ID {
			Ok(self.store.object_list())
		} else {
			let offset = id * (mem::size_of::<Record>() as u64);
			let list = Tree::new_object_list(self).await?;
			let mut root = Record::default();
			let l = list.read(offset, root.as_mut()).await?;
			debug_assert_eq!(l, mem::size_of::<Record>(), "root wasn't fully read");
			Ok(root)
		}
	}

	/// Update an existing root,
	/// i.e. without resizing the object list.
	async fn set_object_root(&self, id: u64, root: &Record) -> Result<(), Error<D>> {
		if id == OBJECT_LIST_ID {
			self.store.set_object_list(*root);
		} else {
			let offset = id * (mem::size_of::<Record>() as u64);
			let list = Tree::new_object_list(self).await?;
			let l = list.write(offset, root.as_ref()).await?;
			debug_assert_eq!(l, mem::size_of::<Record>(), "root wasn't fully written");
		}
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

	async fn read_object_table(&self, id: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
		let offset = id * (mem::size_of::<Record>() as u64);
		Tree::new_object_list(self).await?.read(offset, buf).await
	}

	/// Create an object.
	pub async fn create(&self) -> Result<Tree<D>, Error<D>> {
		let id = self.alloc_ids(1);
		self.write_object_table(
			id,
			Record { references: 1.into(), ..Default::default() }.as_ref(),
		)
		.await?;
		Tree::new(self, id).await
	}

	/// Create a pair of objects.
	/// The second object has ID + 1.
	pub async fn create_pair(&self) -> Result<(Tree<D>, Tree<D>), Error<D>> {
		let id = self.alloc_ids(2);
		let rec = Record { references: 1.into(), ..Default::default() };
		let mut b = [0; 2 * mem::size_of::<Record>()];
		b[..mem::size_of::<Record>()].copy_from_slice(rec.as_ref());
		b[mem::size_of::<Record>()..].copy_from_slice(rec.as_ref());
		self.write_object_table(id, &b).await?;

		let a = Tree::new(self, id).await?;
		let b = Tree::new(self, id).await?;
		Ok((a, b))
	}

	/// Get an object.
	pub async fn get(&self, id: u64) -> Result<Tree<D>, Error<D>> {
		Tree::new(self, id).await
	}

	/// Destroy a record and it's associated cache entry, if any.
	fn destroy(&self, id: u64, depth: u8, offset: u64, record: &Record) -> Vec<u8> {
		let entry = self.remove_entry(id, depth, offset).expect("invalid entry");

		// Free blocks referenced by record.
		self.store.destroy(record);

		entry.data
	}

	/// Remove an entry from the cache.
	///
	/// This does *not* flush the entry!
	fn remove_entry(&self, id: u64, depth: u8, offset: u64) -> Option<Entry> {
		let data = { &mut *self.data.borrow_mut() };
		let obj = data.data.get_mut(&id)?;
		let entry = obj.data[usize::from(depth)].remove(&offset)?;
		data.lrus.adjust_cache_removed_entry(&entry);
		Some(entry)
	}

	/// Move an object to a specific ID.
	///
	/// The old object is destroyed.
	pub async fn move_object(&self, from: u64, to: u64) -> Result<(), Error<D>> {
		if from == to {
			return Ok(()); // Don't even bother.
		}

		// Free allocations
		self.get(to).await?.resize(0).await?;

		// Copy
		{
			let _locks_from = FlushLock::new(&self.data, from).await;
			let _locks_to = FlushLock::new(&self.data, to).await;
			let rec = self.get_object_root(from).await?;
			self.set_object_root(to, &rec).await?;
		}

		// Move object data & fix LRU entries.
		{
			let data = { &mut *self.data.borrow_mut() };

			let obj = data.data.remove(&from).expect("object not present");
			for level in obj.data.iter() {
				for entry in level.values() {
					data.lrus
						.global
						.lru
						.get_mut(entry.global_index)
						.expect("invalid global LRU index")
						.0 = to;
					if let Some(idx) = entry.write_index {
						data.lrus
							.dirty
							.lru
							.get_mut(idx)
							.expect("invalid write LRU index")
							.0 = to;
					}
				}
			}
			data.data.insert(to, obj);
		}

		// Destroy original object.
		self.write_object_table(from, Record::default().as_ref())
			.await?;

		self.dealloc_id(from);
		Ok(())
	}

	/// Increase the reference count of an object.
	///
	/// This may fail if the reference count is already [`u16::MAX`].
	pub async fn increase_refcount(&self, id: u64) -> Result<(), Error<D>> {
		let mut rec = Record::default();
		self.read_object_table(id, rec.as_mut()).await?;
		if rec.references == u16::MAX {
			todo!("too many refs");
		}
		rec.references += 1;
		self.write_object_table(id, rec.as_ref()).await?;
		Ok(())
	}

	/// Decrease the reference count of an object.
	///
	/// If the reference count reaches 0 the object is destroyed.
	pub async fn decrease_refcount(&self, id: u64) -> Result<(), Error<D>> {
		let mut rec = Record::default();
		self.read_object_table(id, rec.as_mut()).await?;
		if rec.references == 0 {
			todo!("invalid object");
		}
		rec.references -= 1;
		self.write_object_table(id, rec.as_ref()).await?;
		Ok(())
	}

	/// Finish the current transaction, committing any changes to the underlying devices.
	pub async fn finish_transaction(&self) -> Result<(), Error<D>> {
		self.store.finish_transaction().await
	}

	/// The block size used by the underlying [`Store`].
	pub fn block_size(&self) -> BlockSize {
		self.store.block_size()
	}

	/// The maximum record size used by the underlying [`Store`].
	pub fn max_record_size(&self) -> MaxRecordSize {
		self.store.max_record_size()
	}

	/// Lock a cache entry, preventing it from being evicted.
	///
	/// Returns `true` if the cache entry was already locked, `false` otherwise.
	fn lock_entry(&self, id: u64, depth: u8, offset: u64) -> CacheRef<D> {
		CacheRef::new(self, id, depth, offset)
	}

	/// Fetch a record for a cache entry.
	///
	/// If the entry is already being fetched,
	/// the caller is instead added to a list to be waken up when the fetcher has finished.
	async fn fetch_entry(
		&self,
		id: u64,
		depth: u8,
		offset: u64,
		record: &Record,
	) -> Result<CacheRef<D>, Error<D>> {
		// Lock now so the entry doesn't get fetched and evicted midway.
		let entry = self.lock_entry(id, depth, offset);

		if self.has_entry(id, depth, offset) {
			return Ok(entry);
		}

		let mut data = self.data.borrow_mut();
		match data.fetching.entry((id, depth, offset)) {
			hash_map::Entry::Vacant(e) => {
				// Add list for other fetchers to register wakers to.
				e.insert(Default::default());
				drop(data);

				// Fetch record
				// This will be polled in the if branch below
				// FIXME if we return here other fetchers may end up waiting indefinitely.
				let d = self.store.read(record).await?;

				let len = d.len();

				let data = { &mut *self.data.borrow_mut() };
				let prev = data.data.get_mut(&id).expect("object does not exist").data
					[usize::from(depth)]
				.insert(
					offset,
					Entry {
						data: d,
						global_index: data.lrus.global.lru.insert((id, depth, offset)),
						write_index: None,
					},
				);
				debug_assert!(prev.is_none(), "entry was already present");
				data.lrus.global.cache_size += len + CACHE_ENTRY_FIXED_COST;

				// Wake other tasks waiting for this entry.
				data.fetching
					.remove(&(id, depth, offset))
					.expect("no wakers list")
					.into_iter()
					.for_each(|w| w.wake());

				// Done
				Ok(entry)
			}
			hash_map::Entry::Occupied(_) => {
				// Wait until the fetcher for this record finishes.
				drop(data);

				let mut entry = Some(entry);

				// TODO how should we deal with errors that may occur in the fetcher?
				// TODO kinda ugly IMO.
				// Is there a cleaner way to write this, without poll_fn perhaps?
				future::poll_fn(move |cx| {
					if self.has_entry(id, depth, offset) {
						Poll::Ready(Ok(entry.take().expect("poll after finish")))
					} else {
						let mut data = self.data.borrow_mut();
						let wakers = data
							.fetching
							.get_mut(&(id, depth, offset))
							.expect("no wakers list");
						if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
							wakers.push(cx.waker().clone());
						}
						Poll::Pending
					}
				})
				.await
			}
		}
	}

	/// Check if a cache entry is present.
	fn has_entry(&self, id: u64, depth: u8, offset: u64) -> bool {
		self.data
			.borrow()
			.data
			.get(&id)
			.and_then(|m| m.data[usize::from(depth)].get(&offset))
			.is_some()
	}

	/// Get a mutable reference to an object's data.
	///
	/// # Panics
	///
	/// If another borrow is alive.
	///
	/// If the object is not present.
	fn get_object_entry_mut(&self, id: u64) -> RefMut<TreeData> {
		RefMut::map(self.data.borrow_mut(), |data| {
			data.data
				.get_mut(&id)
				.expect("cache entry by id does not exist")
		})
	}

	/// Get a cached entry.
	///
	/// # Panics
	///
	/// If another borrow is alive.
	///
	/// If the entry is not present.
	fn get_entry(&self, id: u64, depth: u8, offset: u64) -> Ref<Entry> {
		Ref::map(self.data.borrow(), |data| {
			data.data
				.get(&id)
				.expect("cache entry by id does not exist")
				.data[usize::from(depth)]
			.get(&offset)
			.expect("cache entry by offset does not exist")
		})
	}

	/// Get a cached entry.
	///
	/// Unlike [`Self::get_entry_mut`], this does *not* mark the entry as dirty and hence won't
	/// trigger a flush.
	///
	/// # Panics
	///
	/// If another borrow is alive.
	///
	/// If the entry is not present.
	fn get_entry_mut_no_mark(&self, id: u64, depth: u8, offset: u64) -> RefMut<Entry> {
		RefMut::map(self.get_object_entry_mut(id), |obj| {
			obj.data[usize::from(depth)]
				.get_mut(&offset)
				.expect("cache entry by offset does not exist")
		})
	}

	/// Get a cached entry.
	///
	/// When the [`EntryRefMut`] is dropped the entry will be marked as dirty.
	/// This may later trigger a flush.
	///
	/// # Panics
	///
	/// If another borrow is alive.
	///
	/// If the entry is not present.
	async fn get_entry_mut(
		&self,
		id: u64,
		depth: u8,
		offset: u64,
	) -> Result<EntryRefMut, Error<D>> {
		// Flush since we may be exceeding write cache limits.
		self.flush().await?;

		let (entry, lrus) = RefMut::map_split(self.data.borrow_mut(), |data| {
			let entry = data
				.data
				.get_mut(&id)
				.expect("cache entry by id does not exist")
				.data[usize::from(depth)]
			.get_mut(&offset)
			.expect("cache entry by offset does not exist");
			(entry, &mut data.lrus)
		});

		Ok(EntryRefMut { original_len: entry.data.len(), entry, lrus, id, depth, offset })
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

		while data.lrus.dirty.cache_size > data.lrus.dirty.cache_max {
			// Remove last written to entry.
			let &(id, depth, offset) = data
				.lrus
				.dirty
				.lru
				.last()
				.expect("no nodes despite non-zero write cache size");

			if data
				.data
				.get(&id)
				.expect("invalid object")
				.is_flush_locked()
			{
				break; // TODO continue with another record or object.
			}

			data.lrus.dirty.lru.remove_last().unwrap();

			drop(data);
			let mut entry = self.get_entry_mut_no_mark(id, depth, offset);

			// Store record.
			// TODO try to do concurrent writes.
			let rec = self.store.write(&entry.data).await?;

			// Remove from write LRU
			entry.write_index = None;
			let len = entry.data.len();
			drop(entry);
			self.data.borrow_mut().lrus.dirty.cache_size -= len + CACHE_ENTRY_FIXED_COST;

			// Store the record in the appropriate place.
			let obj = Tree::new(self, id).await?;
			obj.update_record(depth, offset, rec).await?;
			drop(obj);
			data = self.data.borrow_mut();
		}

		while data.lrus.global.cache_size > data.lrus.global.cache_max {
			// Remove last written to entry.
			let &(id, depth, offset) = data
				.lrus
				.global
				.lru
				.last()
				.expect("no nodes despite non-zero write cache size");

			if data
				.data
				.get(&id)
				.expect("invalid object")
				.is_flush_locked()
			{
				break; // TODO continue with another record or object.
			}

			if data.locked_records.contains_key(&(id, depth, offset)) {
				break; // TODO meh
				todo!("don't flush locked records");
			}

			drop(data);
			let entry = self
				.remove_entry(id, depth, offset)
				.expect("entry not present");

			if entry.write_index.is_some() {
				// Store record.
				// TODO try to do concurrent writes.
				let rec = self.store.write(&entry.data).await?;

				// Store the record in the appropriate place.
				let obj = Tree::new(self, id).await?;
				obj.update_record(depth, offset, rec).await?;
			}

			data = self.data.borrow_mut();
		}

		data.is_flushing = false;
		Ok(())
	}

	/// Unmount the cache.
	///
	/// The cache is flushed before returning the underlying [`Store`].
	pub async fn unmount(self) -> Result<Store<D>, Error<D>> {
		trace!("unmount");
		let global_max = self.data.borrow().lrus.global.cache_max;
		self.resize_cache(global_max, 0).await?;
		debug_assert_eq!(
			self.cache_status().dirty_usage,
			0,
			"not all data has been flushed"
		);
		Ok(self.store)
	}

	/// Get cache status
	pub fn cache_status(&self) -> CacheStatus {
		#[cfg(test)]
		self.verify_cache_usage();

		let data = self.data.borrow();
		CacheStatus {
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
			.flat_map(|m| m.values())
			.map(|v| v.data.len() + CACHE_ENTRY_FIXED_COST)
			.sum::<usize>();
		assert_eq!(
			real_global_usage, data.lrus.global.cache_size,
			"global cache size mismatch"
		);
	}
}

/// A cache entry.
///
/// It is a relatively cheap way to avoid lifetimes while helping ensure consistency.
///
/// Cache entries referenced by this structure cannot be removed until all corresponding
/// `CacheRef`s are dropped.
///
/// # Note
///
/// `CacheRef` is safe to hold across `await` points.
struct CacheRef<'a, D: Dev> {
	cache: &'a Cache<D>,
	id: u64,
	depth: u8,
	offset: u64,
}

impl<'a, D: Dev> CacheRef<'a, D> {
	/// Create a new reference to a cache entry.
	///
	/// Returns `true` if the cache entry was already locked, `false` otherwise.
	fn new(cache: &'a Cache<D>, id: u64, depth: u8, offset: u64) -> Self {
		*cache
			.data
			.borrow_mut()
			.locked_records
			.entry((id, depth, offset))
			.or_default() += 1;
		Self { cache, id, depth, offset }
	}

	/// Get a mutable reference to the data.
	///
	/// This will mark the entry as dirty, which in turn may trigger a flush of other dirty
	/// entries.
	///
	/// # Note
	///
	/// The reference **must not** be held across `await` points!
	///
	/// # Panics
	///
	/// If something is already borrowing the underlying [`TreeData`].
	async fn get_mut(&self) -> Result<EntryRefMut, Error<D>> {
		self.cache
			.get_entry_mut(self.id, self.depth, self.offset)
			.await
	}

	/// Get an immutable reference to the data.
	///
	/// # Note
	///
	/// The reference **must not** be held across `await` points!
	///
	/// # Panics
	///
	/// If something is already borrowing the underlying [`TreeData`].
	fn get(&self) -> Ref<Entry> {
		self.cache.get_entry(self.id, self.depth, self.offset)
	}

	/// Explicitly mark this entry as dirty.
	///
	/// # Panics
	///
	/// If something is already borrowing the underlying [`TreeData`].
	async fn mark_dirty(&self) -> Result<(), Error<D>> {
		self.get_mut().await.map(|_| ())
	}

	/// Destroy the record associated with this entry.
	///
	/// # Panics
	///
	/// If something is already borrowing the underlying [`TreeData`].
	fn destroy(self, record: &Record) -> Vec<u8> {
		let cache = self.cache.clone();
		let &CacheRef { id, depth, offset, .. } = &self;
		drop(self);
		cache.destroy(id, depth, offset, record)
	}
}

impl<D: Dev> Drop for CacheRef<'_, D> {
	fn drop(&mut self) {
		let mut tree = self.cache.data.borrow_mut();
		let key = (self.id, self.depth, self.offset);
		let count = tree
			.locked_records
			.get_mut(&key)
			.expect("record entry is not locked");
		*count -= 1;
		if *count == 0 {
			tree.locked_records.remove(&key);
		}
	}
}

impl<D: Dev> fmt::Debug for CacheRef<'_, D> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(CacheRef))
			.field("cache", &format_args!("{{ ... }}"))
			.field("id", &self.id)
			.field("depth", &self.depth)
			.field("offset", &self.offset)
			.finish()
	}
}

/// A single cache entry.
struct Entry {
	/// The data itself.
	data: Vec<u8>,
	/// Global LRU index.
	global_index: lru::Idx,
	/// Dirty LRU index, if the data is actually dirty.
	write_index: Option<lru::Idx>,
}

impl fmt::Debug for Entry {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Entry))
			.field("data", &format_args!("{:?}", &self.data))
			.field("global_index", &self.global_index)
			.field("write_index", &self.write_index)
			.finish()
	}
}

/// Summary of [`Cache`] status.
///
/// Returned by [`Cache::cache_status`].
#[derive(Debug)]
pub struct CacheStatus {
	/// Total amount of memory used by record data, including dirty data.
	pub global_usage: usize,
	/// Total amount of memory used by dirty record data.
	pub dirty_usage: usize,
}

/// Mutable reference to a cache entry.
///
/// This will mark the entry as dirty when it is dropped.
struct EntryRefMut<'a> {
	/// The length of the entry at the time of borrowing.
	original_len: usize,

	id: u64,
	depth: u8,
	offset: u64,
	entry: RefMut<'a, Entry>,
	lrus: RefMut<'a, Lrus>,
}

impl Deref for EntryRefMut<'_> {
	type Target = Entry;

	fn deref(&self) -> &Entry {
		&self.entry
	}
}

impl DerefMut for EntryRefMut<'_> {
	fn deref_mut(&mut self) -> &mut Entry {
		&mut self.entry
	}
}

// TODO async drop would be nice.
impl Drop for EntryRefMut<'_> {
	fn drop(&mut self) {
		// Trim zeros, which we always want to do.
		trim_zeros_end(&mut self.entry.data);

		// Check if we still need to mark the entry as dirty.
		// Otherwise promote.
		if let Some(idx) = self.entry.write_index {
			self.lrus.dirty.lru.promote(idx);
			self.lrus.dirty.cache_size += self.entry.data.len();
			self.lrus.dirty.cache_size -= self.original_len;
		} else {
			let idx = self
				.lrus
				.dirty
				.lru
				.insert((self.id, self.depth, self.offset));
			self.lrus.dirty.cache_size += self.entry.data.len() + CACHE_ENTRY_FIXED_COST;
			self.entry.write_index = Some(idx);
		}
		self.lrus.global.cache_size += self.entry.data.len();
		self.lrus.global.cache_size -= self.original_len;
	}
}
