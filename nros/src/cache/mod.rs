mod busy;
mod entry;
mod evict;
mod flush;
mod mem;
mod object;
mod tree;

pub use object::Object;

use {
	crate::{
		data::record::Depth, resource::Buf, storage, util, Background, BlockSize, Dev, Error,
		KeyDeriver, MaxRecordSize, Resource, Store,
	},
	alloc::collections::{BTreeMap, BTreeSet},
	core::{
		cell::{Cell, RefCell, RefMut},
		fmt,
		future::Future,
		pin::pin,
		task::Waker,
	},
	entry::{Entry, EntryRef},
	futures_util::FutureExt,
	mem::Mem,
	object::{Key, RootIndex},
	rangemap::RangeSet,
	tree::Tree,
};

/// Fixed ID for the object list so it can use the same caching mechanisms as regular objects.
const OBJECT_LIST_ID: u64 = 1 << 58 | 0; // 2**64 / 2**6 = 2**58, ergo 2**58 is just out of range.

/// Fixed ID for the object list so it can use the same caching mechanisms as regular objects.
const OBJECT_BITMAP_ID: u64 = 1 << 58 | 1; // Ditto

/// Record reference size as a power-of-two.
const RECORDREF_SIZE_P2: u8 = 3;

/// Object size as a power-of-two.
const OBJECT_SIZE_P2: u8 = 5;

/// Cache data.
struct CacheData<B: Buf> {
	/// Cached records.
	records: BTreeMap<IdKey, Entry<B>>,
	/// Records that are dirty.
	dirty: BTreeSet<IdKey>,
	/// Busy records.
	busy: busy::BusyMap,
	/// Used object IDs.
	used_objects_ids: RangeSet<u64>,
	/// Evict tasks in progress.
	evict_tasks_count: usize,
	/// Task to wake if evict tasks reaches 0.
	wake_after_evicts: Option<Waker>,
	/// Memory management.
	mem: Mem,
}

impl<B: Buf> CacheData<B> {
	/// Deallocate a single object ID.
	fn dealloc_id(&mut self, id: u64) {
		debug_assert!(self.used_objects_ids.contains(&id), "double free");
		self.used_objects_ids.remove(id..id + 1);
	}
}

impl<B: Buf> fmt::Debug for CacheData<B> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(CacheData))
			.field("records", &self.records)
			.field("dirty", &self.dirty)
			.field("busy", &self.busy)
			.field("used_objects_ids", &self.used_objects_ids)
			.field("evict_tasks_count", &self.evict_tasks_count)
			.field("wake_after_evicts", &self.wake_after_evicts)
			.field("mem", &self.mem)
			.finish()
	}
}

/// Key addressing both object and record.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct IdKey {
	id: u64,
	key: Key,
}

impl fmt::Debug for IdKey {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let &Self { id, key: k } = self;
		format_args!("({:#x}:{:?}:{:?}:{})", id, k.root(), k.depth(), k.offset()).fmt(f)
	}
}

/// Cache algorithm.
pub(crate) struct Cache<D: Dev, R: Resource> {
	/// The non-volatile backing store.
	store: Store<D, R>,
	/// The cached data.
	data: RefCell<CacheData<R::Buf>>,
	/// Precalculated maximum sizes of each object root, in terms of bytes.
	root_max_size: [u64; 4],
	/// The depth of the object bitmap tree.
	///
	/// Derived from the depth of the object list tree.
	object_bitmap_depth: Cell<Depth>,
}

impl<D: Dev + fmt::Debug, R: Resource + fmt::Debug> fmt::Debug for Cache<D, R> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(CacheData))
			.field("store", &self.store)
			.field("data", &self.data)
			.field("root_max_size", &self.root_max_size)
			.field("object_bitmap_depth", &self.object_bitmap_depth)
			.finish()
	}
}

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Initialize a cache layer.
	pub async fn new(store: Store<D, R>, cache_size: usize) -> Result<Self, Error<D>> {
		trace!("new {}", cache_size);

		// TODO don't hardcode, make user-configurable.
		let soft_limit = cache_size >> store.max_rec_size().to_raw();
		let hard_limit = soft_limit * 2 + 1 + 1; // 1 to ensure operation + 1 for grow_object_list
		trace!(info "soft/hard limit: {}/{}", soft_limit, hard_limit);

		let mut root_max_size = [0; 4];
		root_max_size[0] = 1 << store.max_rec_size().to_raw();
		for i in 1..4 {
			root_max_size[i] =
				root_max_size[i - 1] << store.max_rec_size().to_raw() - RECORDREF_SIZE_P2;
		}

		let mut s = Self {
			store,
			data: RefCell::new(CacheData {
				records: Default::default(),
				dirty: Default::default(),
				busy: Default::default(),
				mem: Mem::new(soft_limit, hard_limit),
				used_objects_ids: Default::default(),
				evict_tasks_count: 0,
				wake_after_evicts: None,
			}),
			root_max_size,
			object_bitmap_depth: Cell::new(Depth::D0),
		};
		s.object_bitmap_depth
			.set(s.calc_bitmap_depth(s.store.object_list_depth()));

		// Scan bitmap for empty slots.
		//let bg = Background::default();
		let mut used_objects_ids = RangeSet::new();
		let bitmap = Tree::object_bitmap(&s);
		let entries_per_leaf = 4 << s.max_rec_size().to_raw();

		s.run(async {
			for offset in 0..bitmap.max_offset() {
				let entry = bitmap.get(Depth::D0, offset).await?;
				let mut id = offset * entries_per_leaf;
				for &byte in entry.as_slice() {
					for k in 0..8 {
						if (byte >> k) & 1 != 0 {
							trace!(info "id {:#x} in use", id);
							used_objects_ids.insert(id..id + 1);
						}
						id += 1;
					}
				}
			}
			Ok(())
		})
		.await?;

		trace!(final "used object ids: {:#x?}", &used_objects_ids);

		s.data.get_mut().used_objects_ids = used_objects_ids;

		Ok(s)
	}

	/// Run task with a background runner.
	pub async fn run<V, E, F>(&self, f: F) -> Result<V, E>
	where
		F: Future<Output = Result<V, E>>,
		E: From<Error<D>>,
	{
		let bg = Background::default();
		let mut bg_runner = pin!(self.evict_excess(&bg).fuse());

		let r = {
			futures_util::select_biased! {
				r = bg_runner => r,
				r = pin!(bg.process().fuse()) => r?,
				r = pin!(f.fuse()) => r?,
			}
		};

		futures_util::select_biased! {
			r = bg_runner => r,
			r = pin!(bg.try_run_all().fuse()) => r?,
		};

		Ok(r)
	}

	/// Allocate an object IDs.
	fn alloc_id(&self) -> u64 {
		let mut slf = self.data();
		let id = slf
			.used_objects_ids
			.gaps(&(0..u64::MAX))
			.next()
			.expect("more than 2**64 objects allocated")
			.start;
		slf.used_objects_ids.insert(id..id + 1);
		id
	}

	/// Create an object.
	pub async fn create(&self) -> Result<Object<'_, D, R>, Error<D>> {
		trace!("create");
		// Allocate
		let id = self.alloc_id();
		trace!(info "{:#x}", id);

		// Resize if necessary
		if id >= self.object_list_len() {
			self.grow_object_list().await?;
		}

		self.object_set_allocated(id, true).await?;

		Ok(Object::new(self, id))
	}

	/// Get a reference to an object.
	pub fn get(&self, id: u64) -> Object<'_, D, R> {
		Object::new(self, id)
	}

	/// Finish the current transaction, committing any changes to the underlying devices.
	pub async fn finish_transaction(&self) -> Result<(), Error<D>> {
		// First flush cache
		self.flush_all().await?;

		// Flush store-specific data.
		self.store.finish_transaction().await?;

		Ok(())
	}

	/// The block size used by the underlying [`Store`].
	pub fn block_size(&self) -> BlockSize {
		self.store.block_size()
	}

	/// The maximum record size used by the underlying [`Store`].
	pub fn max_rec_size(&self) -> MaxRecordSize {
		self.store.max_rec_size()
	}

	/// Readjust cache size.
	///
	/// This may be useful to increase or decrease depending on total system memory usage.
	///
	/// # Panics
	///
	/// If `global_max < write_max`.
	pub fn resize_cache(&self, global_max: usize) -> Result<(), Error<D>> {
		let soft_limit = global_max >> self.max_rec_size().to_raw();
		self.data().mem.set_soft_limit(soft_limit);
		Ok(())
	}

	/// Unmount the cache.
	///
	/// The cache is flushed before returning the underlying [`Store`].
	pub async fn unmount(self) -> Result<Store<D, R>, Error<D>> {
		trace!("unmount");
		self.run(self.finish_transaction()).await?;
		Ok(self.store)
	}

	/// Get statistics for this sesion.
	pub fn statistics(&self) -> Statistics {
		let d = self.data();
		Statistics {
			storage: self.store.statistics(),
			soft_usage: d.mem.soft_count() << self.max_rec_size().to_raw(),
			hard_usage: d.mem.hard_count() << self.max_rec_size().to_raw(),
			used_objects: d
				.used_objects_ids
				.iter()
				.fold(0, |x, r| r.end - r.start + x),
		}
	}

	/// Get the key used to encrypt the header.
	pub fn header_key(&self) -> [u8; 32] {
		self.store.header_key()
	}

	/// Set a new key derivation function.
	///
	/// This replaces the header key.
	pub fn set_key_deriver(&self, kdf: KeyDeriver<'_>) {
		self.store.set_key_deriver(kdf)
	}

	/// Amount of entries in a parent record as a power of two.
	fn entries_per_parent_p2(&self) -> u8 {
		self.max_rec_size().to_raw() - RECORDREF_SIZE_P2
	}

	fn resource(&self) -> &R {
		self.store.resource()
	}

	/// The maximum amount of entries the object list can contain with its current depth.
	fn object_list_len(&self) -> u64 {
		Tree::object_list(self).max_offset() << self.max_rec_size().to_raw() - OBJECT_SIZE_P2
	}

	fn data(&self) -> RefMut<'_, CacheData<R::Buf>> {
		self.data.borrow_mut()
	}

	fn mem(&self) -> RefMut<'_, Mem> {
		RefMut::map(self.data(), |d| &mut d.mem)
	}
}

/// Statistics for this session.
///
/// Used for debugging.
#[derive(Clone, Copy, Debug, Default)]
pub struct Statistics {
	/// Storage statistics.
	pub storage: storage::Statistics,
	/// Amount of bytes counting towards the soft limit.
	pub soft_usage: usize,
	/// Amount of bytes counting towards the hard limit.
	pub hard_usage: usize,
	/// Total amount of objects allocated.
	pub used_objects: u64,
}

#[cfg(feature = "trace")]
impl<B: Buf> Drop for CacheData<B> {
	fn drop(&mut self) {
		if std::thread::panicking() {
			eprintln!("state: {:#?}", self);
		}
	}
}
