mod tree;
mod lru;

use tree::TreeCache;

use {
	crate::{Dev, Store, RecordTree, Record, Error},
	alloc::collections::BTreeMap,
	core::{cmp::Ordering, ptr::NonNull, mem},
	std::rc::Rc,
	core::cell::{RefCell, RefMut},
	rustc_hash::FxHashMap,
	rangemap::RangeSet,
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
pub(crate) struct CacheData<D: Dev> {
	/// The non-volatile backing store.
	store: Store<D>,
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
	/// Used object IDs.
	used_objects_ids: RangeSet<u64>,
}

/// Cache algorithm.
#[derive(Debug)]
pub(crate) struct Cache<D: Dev>(Rc<RefCell<CacheData<D>>>);

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
		Self(Rc::new(RefCell::new(CacheData {
			store,
			data: Default::default(),
			global_lru: Default::default(),
			write_lru: Default::default(),
			global_cache_max,
			write_cache_max,
			global_cache_size: 0,
			write_cache_size: 0,
			used_objects_ids: Default::default(),
		})))
	}

	/// Allocate an arbitrary amount of object IDs.
	fn alloc_ids(&self, count: u64) -> u64 {
		let mut slf = self.0.borrow_mut();
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
		let mut slf = self.borrow_mut();
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
	pub fn block_size_p2(&self) -> u8 {
		self.store.block_size_p2()
	}
}
