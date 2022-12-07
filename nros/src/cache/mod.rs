mod lru;
mod tree;

use std::{borrow::BorrowMut, string};

pub use tree::Tree;

use {
	crate::{BlockSize, Dev, Error, MaxRecordSize, Record, Store},
	alloc::collections::BTreeMap,
	core::{
		cell::{Ref, RefCell, RefMut},
		cmp::Ordering,
		fmt,
		future::{self, Future},
		mem,
		pin::Pin,
		ptr::NonNull,
		task::{Context, Poll, Waker},
	},
	rangemap::RangeSet,
	rustc_hash::FxHashMap,
	std::{collections::hash_map, rc::Rc},
};

/// Key to a cache entry.
struct Key {
	object_id: u64,
	offset: u64,
}

/// Fixed ID for the object list so it can use the same caching mechanisms as regular objects.
const OBJECT_LIST_ID: u64 = u64::MAX;

/// A single cached record tree.
#[derive(Debug)]
struct TreeData {
	/// The current length of the tree.
	///
	/// This may not match the length stored in the root if the tree was resized.
	length: u64,
	/// Cached records.
	///
	/// The index in the array is correlated with depth.
	/// The key is correlated with offset.
	data: Box<[FxHashMap<u64, Entry>]>,
}

/// Cache data.
#[derive(Debug)]
pub(crate) struct CacheData {
	/// Cached records of objects and the object list.
	///
	/// The key, in order, is `(id, depth, offset)`.
	/// Using separate hashmaps allows using only a prefix of the key.
	data: FxHashMap<u64, TreeData>,
	/// Linked list for global LRU evictions.
	global_lru: lru::LruList<(u64, u8, u64)>,
	/// Linked list for flushing dirty records.
	///
	/// This is not used for eviction but ensures an excessive amount of writes does not hold up
	/// reads.
	// TODO consider adding a sort of dirty map so records are grouped by object & sorted,
	// which may improve read performance.
	write_lru: lru::LruList<(u64, u8, u64)>,
	/// The maximum amount of total bytes to keep cache.
	global_cache_max: usize,
	/// The maximum amount of dirty bytes to keep.
	write_cache_max: usize,
	/// The total amount of cached bytes.
	global_cache_size: usize,
	/// The total amount of dirty cached bytes.
	write_cache_size: usize,
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
	/// If `global_cache_max` is smaller than `write_cache_max`.
	///
	/// If `write_cache_max` is smaller than the maximum record size.
	pub fn new(store: Store<D>, global_cache_max: usize, write_cache_max: usize) -> Self {
		assert!(
			global_cache_max >= write_cache_max,
			"global cache size is smaller than write cache"
		);
		assert!(
			write_cache_max >= 1 << store.max_record_size().to_raw(),
			"write cache size is smaller than the maximum record size"
		);
		Self {
			store,
			data: RefCell::new(CacheData {
				data: Default::default(),
				global_lru: Default::default(),
				write_lru: Default::default(),
				global_cache_max,
				write_cache_max,
				global_cache_size: 0,
				write_cache_size: 0,
				locked_objects: Default::default(),
				locked_records: Default::default(),
				fetching: Default::default(),
				used_objects_ids: Default::default(),
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

	async fn write_object_table(self: Rc<Self>, id: u64, data: &[u8]) -> Result<usize, Error<D>> {
		let offset = id * (mem::size_of::<Record>() as u64);
		let min_len = offset + u64::try_from(data.len()).unwrap();

		let list = Tree::new_object_list(self).await?;

		if min_len > list.len().await? {
			list.resize(min_len).await?;
		}

		list.write(offset, data).await
	}

	async fn read_object_table(self: Rc<Self>, id: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
		let offset = id * (mem::size_of::<Record>() as u64);
		Tree::new_object_list(self).await?.read(offset, buf).await
	}

	/// Create an object.
	pub async fn create(self: Rc<Self>) -> Result<Tree<D>, Error<D>> {
		let id = self.alloc_ids(1);
		self.clone()
			.write_object_table(
				id,
				Record { references: 1.into(), ..Default::default() }.as_ref(),
			)
			.await?;
		Tree::new(self, id).await
	}

	/// Create a pair of objects.
	/// The second object has ID + 1.
	pub async fn create_pair(self: Rc<Self>) -> Result<(Tree<D>, Tree<D>), Error<D>> {
		let id = self.alloc_ids(2);
		let rec = Record { references: 1.into(), ..Default::default() };
		let mut b = [0; 2 * mem::size_of::<Record>()];
		b[..mem::size_of::<Record>()].copy_from_slice(rec.as_ref());
		b[mem::size_of::<Record>()..].copy_from_slice(rec.as_ref());
		self.clone().write_object_table(id, &b).await?;

		let a = Tree::new(self.clone(), id).await?;
		let b = Tree::new(self, id).await?;
		Ok((a, b))
	}

	/// Get an object.
	pub async fn get(self: Rc<Self>, id: u64) -> Result<Tree<D>, Error<D>> {
		Tree::new(self, id).await
	}

	/// Destroy a record and it's associated cache entry, if any.
	fn destroy(&self, id: u64, depth: u8, offset: u64, record: &Record) {
		let _ = self.remove_entry(id, depth, offset);

		// Free blocks referenced by record.
		self.store.destroy(record)
	}

	/// Remove an entry from the cache.
	///
	/// This does *not* flush the entry!
	fn remove_entry(&self, id: u64, depth: u8, offset: u64) -> Option<Entry> {
		let data = { &mut *self.data.borrow_mut() };
		let obj = data.data.get_mut(&id)?;
		let entry = obj.data[usize::from(depth)].remove(&offset)?;

		// Remove entry from LRUs
		let len = entry.data.len(); // TODO len() or capacity()?;
		data.global_lru.remove(entry.global_index);
		data.global_cache_size -= len;
		if let Some(idx) = entry.write_index {
			data.write_lru.remove(idx);
			data.write_cache_size -= len;
		}

		// TODO remove the object at an appropriate time.
		// i.e. right after the root has been saved and there are no remaining cached records.

		Some(entry)
	}

	/// Move an object to a specific ID.
	///
	/// The old object is destroyed.
	pub async fn move_object(self: Rc<Self>, from: u64, to: u64) -> Result<(), Error<D>> {
		let mut rec = Record::default();
		// Free allocations
		self.clone().get(to).await?.resize(0).await?;
		// Copy
		let l = self.clone().read_object_table(from, rec.as_mut()).await?;
		assert!(l > 0, "ID out of range");
		let l = self.clone().write_object_table(to, rec.as_ref()).await?;
		assert!(l > 0, "ID out of range");
		// Destroy original object
		self.clone()
			.write_object_table(from, Record::default().as_ref())
			.await?;
		self.dealloc_id(from);
		Ok(())
	}

	/// Increase the reference count of an object.
	///
	/// This may fail if the reference count is already [`u16::MAX`].
	pub async fn increase_refcount(self: Rc<Self>, id: u64) -> Result<(), Error<D>> {
		let mut rec = Record::default();
		self.clone().read_object_table(id, rec.as_mut()).await?;
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
	pub async fn decrease_refcount(self: Rc<Self>, id: u64) -> Result<(), Error<D>> {
		let mut rec = Record::default();
		self.clone().read_object_table(id, rec.as_mut()).await?;
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
	fn lock_entry(self: Rc<Self>, id: u64, depth: u8, offset: u64) -> CacheRef<D> {
		CacheRef::new(self, id, depth, offset)
	}

	/// Fetch a record for a cache entry.
	///
	/// If the entry is already being fetched,
	/// the caller is instead added to a list to be waken up when the fetcher has finished.
	async fn fetch_entry(
		self: Rc<Self>,
		id: u64,
		depth: u8,
		offset: u64,
		record: &Record,
	) -> Result<CacheRef<D>, Error<D>> {
		// Lock now so the entry doesn't get fetched and evicted midway.
		let entry = self.clone().lock_entry(id, depth, offset);

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
						global_index: data.global_lru.insert((id, depth, offset)),
						write_index: None,
					},
				);
				debug_assert!(prev.is_none(), "entry was already present");
				data.global_cache_size += len;

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
					if self.clone().has_entry(id, depth, offset) {
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
		RefMut::map(self.data.borrow_mut(), |data| {
			data.data
				.get_mut(&id)
				.expect("cache entry by id does not exist")
				.data[usize::from(depth)]
			.get_mut(&offset)
			.expect("cache entry by offset does not exist")
		})
	}

	/// Get a cached entry.
	///
	/// This will mark the entry as dirty, which in turn may trigger a flush of other dirty
	/// entries.
	///
	/// # Panics
	///
	/// If another borrow is alive.
	///
	/// If the entry is not present.
	async fn get_entry_mut(
		//self: Rc<Self>,
		&self,
		id: u64,
		depth: u8,
		offset: u64,
	) -> Result<RefMut<Entry>, Error<D>> {
		let mut result = Ok(());

		let entry = RefMut::map(self.data.borrow_mut(), |data| {
			let entry = data
				.data
				.get_mut(&id)
				.expect("cache entry by id does not exist")
				.data[usize::from(depth)]
			.get_mut(&offset)
			.expect("cache entry by offset does not exist");

			if let Some(idx) = entry.write_index {
				// Already dirty, bump to front
				data.write_lru.promote(idx);
			} else {
				// Not dirty yet, add to list.
				let key = *data
					.global_lru
					.get(entry.global_index)
					.expect("entry not in global LRU");
				entry.write_index = Some(data.write_lru.insert(key));

				// TODO should we use len() or capacity()?
				// Maybe shrink data too from time to time?
				data.write_cache_size += entry.data.len();
			}

			entry
		});

		// Flush since we may be exceeding write cache limits.
		// TODO figure out how to flush and still make the borrow stuff work.
		//self.clone().flush().await?;

		result.map(|()| entry)
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
	pub async fn resize_cache(
		self: Rc<Self>,
		global_max: usize,
		write_max: usize,
	) -> Result<(), Error<D>> {
		assert!(
			global_max >= write_max,
			"global cache is smaller than write cache"
		);
		{
			let mut data = { &mut *self.data.borrow_mut() };
			data.global_cache_max = global_max;
			data.write_cache_max = write_max;
		}
		self.flush().await
	}

	/// Recalculate total cache usage from resizing a record and flush if necessary.
	///
	/// This adjusts both read and write cache.
	async fn adjust_cache_use_both(
		self: Rc<Self>,
		old_len: usize,
		new_len: usize,
	) -> Result<(), Error<D>> {
		{
			let data = { &mut *self.data.borrow_mut() };
			data.global_cache_size += new_len;
			data.global_cache_size -= old_len;
			data.write_cache_size += new_len;
			data.write_cache_size -= old_len;
		}
		self.flush().await
	}

	/// Flush if total memory use exceeds limits.
	async fn flush(self: Rc<Self>) -> Result<(), Error<D>> {
		let mut data = self.data.borrow_mut();

		if data.is_flushing {
			// Don't bother.
			return Ok(());
		}
		data.is_flushing = true;

		while data.write_cache_size > data.write_cache_max {
			// Remove last written to entry.
			let (id, depth, offset) = data
				.write_lru
				.remove_last()
				.expect("no nodes despite non-zero write cache size");
			drop(data);
			let mut entry = self.get_entry_mut_no_mark(id, depth, offset);

			// Store record.
			// TODO try to do concurrent writes.
			let rec = self.store.write(&entry.data).await?;

			// Remove from write LRU
			entry.write_index = None;
			let len = entry.data.len();
			drop(entry);
			self.data.borrow_mut().write_cache_size -= len;

			// Store the record in the appropriate place.
			let obj = Tree::new(self.clone(), id).await?;
			obj.update_record(depth, offset, rec).await?;
			drop(obj);
			data = self.data.borrow_mut();
		}

		while data.global_cache_size > data.global_cache_max {
			// Remove last written to entry.
			let &(id, depth, offset) = data
				.global_lru
				.last()
				.expect("no nodes despite non-zero write cache size");
			drop(data);
			let entry = self
				.remove_entry(id, depth, offset)
				.expect("entry not present");

			if entry.write_index.is_some() {
				// Store record.
				// TODO try to do concurrent writes.
				let rec = self.store.write(&entry.data).await?;

				// Store the record in the appropriate place.
				let obj = Tree::new(self.clone(), id).await?;
				obj.update_record(depth, offset, rec).await?;
			}

			data = self.data.borrow_mut();
		}

		data.is_flushing = false;
		Ok(())
	}

	/// Get cache status
	pub fn cache_status(&self) -> CacheStatus {
		#[cfg(test)]
		self.verify_cache_usage();

		let data = self.data.borrow();
		CacheStatus { global_usage: data.global_cache_size, dirty_usage: data.write_cache_size }
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
			.map(|v| v.data.len())
			.sum::<usize>();
		assert_eq!(
			real_global_usage, data.global_cache_size,
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
struct CacheRef<D: Dev> {
	cache: Rc<Cache<D>>,
	id: u64,
	depth: u8,
	offset: u64,
}

impl<D: Dev> CacheRef<D> {
	/// Create a new reference to a cache entry.
	///
	/// Returns `true` if the cache entry was already locked, `false` otherwise.
	fn new(cache: Rc<Cache<D>>, id: u64, depth: u8, offset: u64) -> Self {
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
	async fn get_mut(&self) -> Result<RefMut<Entry>, Error<D>> {
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
}

impl<D: Dev> Drop for CacheRef<D> {
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

impl<D: Dev> fmt::Debug for CacheRef<D> {
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
