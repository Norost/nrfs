mod tree;
mod lru;

use tree::Tree;

use {
	crate::{Dev, Store, RecordTree, Record, Error, BlockSize, MaxRecordSize},
	alloc::collections::BTreeMap,
	core::{cmp::Ordering, ptr::NonNull, mem, task::{Waker, Poll, Context}, future::Future, pin::Pin},
	std::rc::Rc,
	core::cell::{RefCell, RefMut, Ref},
	rustc_hash::FxHashMap,
	rangemap::RangeSet,
	std::collections::hash_map::Entry,
};

/// Key to a cache entry.
struct Key {
	object_id: u64,
	offset: u64,
}

/// Fixed ID for the object list so it can use the same caching mechanisms as regular objects.
const OBJECT_LIST_ID: u64 = u64::MAX;

/// The maximum depth of a record tree.
///
/// A record tree can contain up to 2^64 bytes of data.
/// The maximum record size is 8 KiB = 2^13 bytes.
/// Each record is 32 = 2^5 bytes large.
///
/// Ergo, maximum depth is `ceil((64 - 13) / (13 - 5)) + 1 = 8`, including leaves.
///
/// However, we don't need to include the root itself, so substract one -> `8 - 1 = 7`.
const MAX_DEPTH: u8 = 7;

/// Cache data.
#[derive(Debug)]
pub(crate) struct CacheData {
	/// Cached records of objects and the object list.
	///
	/// The key, in order, is `(id, depth, offset)`.
	/// Using separate hashmaps allows using only a prefix of the key.
	data: FxHashMap<u64, [FxHashMap<u64, Vec<u8>>; MAX_DEPTH as _]>,
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
	/// Cache entries that are currently in use and must not be evicted.
	locked: FxHashMap<(u64, u8, u64), usize>,
	/// Cache entries that are currently being fetched.
	fetching: FxHashMap<(u64, u8, u64), Vec<Waker>>,
	/// Used object IDs.
	used_objects_ids: RangeSet<u64>,
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
		assert!(global_cache_max >= write_cache_max, "global cache size is smaller than write cache");
		assert!(write_cache_max >= 1 << store.max_record_size().to_raw(), "write cache size is smaller than the maximum record size");
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
				locked: Default::default(),
				fetching: Default::default(),
				used_objects_ids: Default::default(),
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

	async fn write_object_table(&mut self, id: u64, data: &[u8]) -> Result<(), Error<D>> {
		let offset = id * (mem::size_of::<RecordTree>() as _);
		let min_len = offset + (data.len() as _);

		if min_len > self.object_table.len() {
			self.object_table.resize(self.store.record_size(), min_len).await?;
		}

		if self.object_table.write(self.store.record_size(), offset, data).await? == 0 {
			// Write failed as we need to fetch data first.
			todo!()
		}

		Ok(())
	}

	async fn read_object_table(&mut self, id: u64, buf: &mut [u8]) -> Result<(), Error<D>> {
		let offset = id * (mem::size_of::<RecordTree>() as _);

		if self.object_table.read(self.store.record_size(), offset, buf).unwrap() == 0 {
			// Write failed as we need to fetch data first.
			todo!()
		}

		Ok(())
	}

	/// Create an object.
	pub async fn create(&mut self) -> Result<u64, Error<D>> {
		let id = self.alloc_ids(1);
		self.write_object_table(id, Record { references: 1.into(), ..Default::default() }.as_ref()).await?;
		Ok(id)
	}

	/// Create a pair of objects.
	/// The second object has ID + 1.
	pub async fn create_pair(&mut self) -> Result<u64, Error<D>> {
		let id = self.alloc_ids(2);
		let rec = Record { references: 1.into(), ..Default::default() };
		let mut b = [0; 2 * mem::size_of::<RecordTree>()];
		b[..mem::size_of::<RecordTree>()].copy_from_slice(rec.as_ref());
		b[mem::size_of::<RecordTree>()..].copy_from_slice(rec.as_ref());
		self.write_object_table(id, &b).await?;
		Ok(id)
	}

	/// Destroy an object.
	pub async fn destroy(&mut self, id: u64) -> Result<(), Error<D>> {
		// Free allocations
		self.resize(id, 0).await?;
		// Mark slot as empty
		self.write_object_table(id, Record::default().as_ref()).await?;
		self.dealloc_id(id);
		Ok(())
	}

	/// Move an object to a specific ID.
	///
	/// The old object is destroyed.
	pub async fn move_object(&mut self, from: u64, to: u64) -> Result<(), Error<D>> {
		let mut rec = Record::default();
		// Free allocations
		self.resize(to, 0).await?;
		// Copy
		self.read_object_table(from, rec.as_mut()).await?;
		self.write_object_table(to, rec.as_ref()).await?;
		// Destroy original object
		self.write_object_table(from, Record::default().as_ref()).await?;
		self.dealloc_id(from);
		Ok(())
	}

	/// Increase the reference count of an object.
	///
	/// This may fail if the reference count is already [`u16::MAX`].
	pub async fn increase_refcount(&mut self, id: u64) -> Result<(), Error<D>> {
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
	pub async fn decrease_refcount(&mut self, id: u64) -> Result<(), Error<D>> {
		let mut rec = Record::default();
		self.read_object_table(id, rec.as_mut()).await?;
		if rec.references == 0 {
			todo!("invalid object");
		}
		rec.references -= 1;
		self.write_object_table(id, rec.as_ref()).await?;
		Ok(())
	}

	/// Resize an object.
	pub async fn resize(&mut self, id: u64, new_len: u64) -> Result<(), Error<D>> {
		if let Some(obj) = self.objects.get_mut(&id) {
			obj.resize(self.store.record_size(), new_len).await
		} else {
			todo!()
		}
	}

	/// Get the length of an object.
	pub async fn object_len(&mut self, id: u64) -> Result<u64, Error<D>> {
		todo!()
	}

	/// Read data.
	pub async fn read(&mut self, id: u64, offset: u64, buf: &mut [u8]) -> Result<(), Error<D>> {
		todo!()
	}

	/// Write data.
	pub async fn write(&mut self, id: u64, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		todo!()
	}

	/// Zeroize data.
	/// This is equivalent to `read(id, offset, &[0; len])` though more efficient as it
	/// can erase large ranges at once.
	pub async fn zeroize(&mut self, id: u64, offset: u64, len: u64) -> Result<(), Error<D>> {
		todo!()
	}

	/// Finish the current transaction, committing any changes to the underlying devices.
	pub async fn finish_transaction(&mut self) -> Result<(), Error<D>> {
		todo!()
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
	fn fetch_entry(self: Rc<Self>, id: u64, depth: u8, offset: u64, record: &Record) -> impl Future<Output = Result<CacheRef<D>, Error<D>>> {
		struct Fetch<D: Dev> {
			cache: Rc<Cache<D>>,
			id: u64,
			depth: u8,
			offset: u64,
		}

		impl<D: Dev> Future for Fetch<D> {
			type Output = Result<CacheRef<D>, Error<D>>;

			fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
				if self.cache.has_entry(self.id, self.depth, self.offset) {
					return Poll::Ready(Ok(CacheRef::new(self.cache, self.id, self.depth, self.offset)));
				}

				let mut cache = self.cache.data.borrow_mut();
				match cache.fetching.entry((self.id, self.depth, self.offset)) {
					Entry::Vacant(e) => {
						e.insert(Default::default());
						drop(cache);
						todo!()
					}
					Entry::Occupied(e) => {
						e.get_mut().push(cx.waker().clone());
					}
				}
				Poll::Pending
			}
		}

		Fetch {
			cache: self,
			id,
			depth,
			offset,
		}
	}

	/// Check if a cache entry is present.
	fn has_entry(&self, id: u64, depth: u8, offset: u64) -> bool {
		self.data
			.borrow()
			.data
			.get(&id)
			.and_then(|m| m[usize::from(depth)].get(&offset))
			.is_some()
	}

	/// Get a cached entry.
	///
	/// # Panics
	///
	/// If another borrow is alive.
	///
	/// If the entry is not present.
	fn get_entry(&self, id: u64, depth: u8, offset: u64) -> RefMut<Vec<u8>> {
		self.get_entry_mut(id, depth, offset)
	}

	/// Get a cached entry.
	///
	/// # Panics
	///
	/// If another borrow is alive.
	///
	/// If the entry is not present.
	fn get_entry_mut(&self, id: u64, depth: u8, offset: u64) -> RefMut<Vec<u8>> {
		RefMut::map(self.data.borrow_mut(), |data| {
			data.data
				.get_mut(&id)
				.expect("cache entry by id does not exist")
				[usize::from(depth)]
				.get_mut(&offset)
				.expect("cache entry by offset does not exist")
		})
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
		*cache.data.borrow_mut().locked.entry((id, depth, offset)).or_default() += 1;
		Self { cache, id, depth, offset }
	}

	/// Get a mutable reference to the data.
	///
	/// # Note
	///
	/// The reference **must not** be held across `await` points!
	///
	/// # Panics
	///
	/// If something is already borrowing the underlying [`TreeData`].
	fn get_mut(&self) -> RefMut<Vec<u8>> {
		self.cache.get_entry_mut(self.id, self.depth, self.offset)
	}
}

impl<D: Dev> Drop for CacheRef<D> {
	fn drop(&mut self) {
		let mut tree = self.cache.data.borrow_mut();
		let key = (self.id, self.depth, self.offset);
		let count = tree.locked.get_mut(&key).expect("cache entry is not locked");
		*count -= 1;
		if *count == 0 {
			tree.locked.remove(&key);
		}
	}
}
