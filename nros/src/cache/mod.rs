mod entry;
mod evict;
mod flush;
mod key;
mod memory_tracker;
mod object;
mod slot;
mod tree;

pub use tree::Tree;

use {
	crate::{
		object::Object,
		resource::Buf,
		storage,
		util::{self, box_fut, BTreeMapExt},
		Background, BlockSize, Dev, Error, KeyDeriver, MaxRecordSize, Record, Resource, Store,
	},
	alloc::collections::BTreeMap,
	core::{
		cell::{RefCell, RefMut},
		fmt,
		future::Future,
		mem,
		num::NonZeroU64,
		pin::pin,
		task::Waker,
	},
	entry::EntryRef,
	futures_util::FutureExt,
	key::Key,
	memory_tracker::MemoryTracker,
	rangemap::RangeSet,
	slot::{Busy, Present, RefCount, Slot, SlotExt},
	tree::data::TreeData,
};

/// Fixed ID for the object list so it can use the same caching mechanisms as regular objects.
const OBJECT_LIST_ID: u64 = 1 << 58 | 0; // 2**64 / 2**6 = 2**58, ergo 2**58 is just out of range.

/// Fixed ID for the object list so it can use the same caching mechanisms as regular objects.
const OBJECT_BITMAP_ID: u64 = 1 << 58 | 1; // Ditto

/// Bit indicating an object is referenced, i.e. in use by upper layers.
pub const OBJECT_BITMAP_INUSE: u8 = 1 << 0;

/// Bit indicating an object is non-zero, i.e. block count and root are not zero.
pub const OBJECT_BITMAP_NONZERO: u8 = 1 << 1;

/// Ratio of object vs bitmap field size.
const OBJECT_BITMAP_FIELD_RATIO: u64 = 4 << OBJECT_SIZE_P2;

/// Ratio of object vs bitmap field size as a power of 2.
const OBJECT_BITMAP_FIELD_RATIO_P2: u8 = 2 + OBJECT_SIZE_P2;

/// Record size as a power-of-two.
const RECORD_SIZE_P2: u8 = mem::size_of::<Record>().ilog2() as _;

/// Object size as a power-of-two.
const OBJECT_SIZE_P2: u8 = mem::size_of::<Object>().ilog2() as _;

/// The ID refers to a pseudo-object or an entry of a pseudo-object.
pub const ID_PSEUDO: u64 = 1 << 59;

/// Cache data.
pub(crate) struct CacheData<B: Buf> {
	/// Cached records of objects and the object list.
	///
	/// The key, in order, is `(id, depth, offset)`.
	/// Using separate hashmaps allows using only a prefix of the key.
	objects: BTreeMap<u64, Slot<TreeData<B>>>,
	/// Memory usage tracker.
	memory_tracker: MemoryTracker,
	/// Used object IDs.
	used_objects_ids: RangeSet<u64>,
	/// Pseudo object ID counter.
	pseudo_id_counter: NonZeroU64,
	/// Evict tasks in progress.
	evict_tasks_count: usize,
	/// Task to wake if evict tasks reaches 0.
	wake_after_evicts: Option<Waker>,
}

impl<B: Buf> CacheData<B> {
	/// Deallocate a single object ID.
	fn dealloc_id(&mut self, id: u64) {
		debug_assert!(self.used_objects_ids.contains(&id), "double free");
		self.used_objects_ids.remove(id..id + 1);
	}

	/// Allocate a pseudo-ID.
	fn new_pseudo_id(&mut self) -> u64 {
		trace!("new_pseudo_id");
		let mut counter @ id = self.pseudo_id_counter.get();
		counter += 1;
		counter %= 1 << 59;
		self.pseudo_id_counter = NonZeroU64::new(counter).unwrap_or(NonZeroU64::MIN);
		trace!(info "{:#x}", id | ID_PSEUDO);
		id | ID_PSEUDO
	}
}

impl<B: Buf> fmt::Debug for CacheData<B> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(CacheData))
			.field("objects", &self.objects)
			.field("memory_tracker", &self.memory_tracker)
			.field("used_objects_ids", &self.used_objects_ids)
			.field("pseudo_id_counter", &self.pseudo_id_counter)
			.finish()
	}
}

/// Cache algorithm.
pub(crate) struct Cache<D: Dev, R: Resource> {
	/// The non-volatile backing store.
	store: Store<D, R>,
	/// The cached data.
	data: RefCell<CacheData<R::Buf>>,
}

impl<D: Dev + fmt::Debug, R: Resource + fmt::Debug> fmt::Debug for Cache<D, R> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(CacheData))
			.field("store", &self.store)
			.field("data", &self.data)
			.finish()
	}
}

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Initialize a cache layer.
	pub async fn new(store: Store<D, R>, cache_size: usize) -> Result<Self, Error<D>> {
		trace!("new {}", cache_size);

		// TODO don't hardcode, make user-configurable.
		//
		// Minimum limit is (max record size + entry fixed cost + object fixed cost) * 2,
		// which is the cost of having exactly one maximum-size entry.
		//
		// Multiply by 4 since saving an object requires fetching an entry from
		// the object list & bitmap consecutively, i.e. we need to be able to
		// have 4 entries in the cache at any time.
		let min_limit = (1 << store.max_record_size().to_raw())
			+ 32 // entry
			+ 128; // obj
		let total_limit = cache_size * 2 + min_limit * 4;
		trace!(info "total usage limit: {}", total_limit);

		let mut s = Self {
			store,
			data: RefCell::new(CacheData {
				objects: Default::default(),
				memory_tracker: MemoryTracker::new(cache_size, total_limit),
				used_objects_ids: Default::default(),
				pseudo_id_counter: NonZeroU64::MIN,
				evict_tasks_count: 0,
				wake_after_evicts: None,
			}),
		};

		// Scan bitmap for empty slots.
		//let bg = Background::default();
		let mut used_objects_ids = RangeSet::new();
		let bitmap = Tree::new(&s, OBJECT_BITMAP_ID);
		let entries_per_leaf = 4 << s.max_record_size().to_raw();

		s.run(async {
			for offset in 0.. {
				let Some(entry) = bitmap.view(offset).await? else { break };
				let mut id = offset * entries_per_leaf;
				for &byte in entry.get() {
					for k in 0..4 {
						let bits = (byte >> k * 2) & 0b11;
						match bits {
							// Free slot
							0b00 => {}
							// Free but non-zero slot
							0b10 => todo!("write zeros to free non-zero slot"),
							// Used slot
							0b01 | 0b11 => {
								trace!(info "id {:#x} in use", id);
								used_objects_ids.insert(id..id + 1);
							}
							_ => unreachable!(),
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

	/// Create an object.
	pub async fn create(&self) -> Result<Tree<'_, D, R>, Error<D>> {
		trace!("create");
		let id = self.create_many(1).await?;
		Ok(Tree::new(self, id))
	}

	/// Create many adjacent objects.
	pub async fn create_many(&self, amount: u64) -> Result<u64, Error<D>> {
		trace!("create_many {}", amount);
		// Allocate
		let id = self.alloc_ids(amount);
		trace!(info "{:#x}", id);

		// Resize if necessary
		// Use while instead of if in case someone tries to crate *a lot*
		// of objects at once, which requires growing many times.
		// While growing to the proper size in one go would be more efficient,
		// this is an easy fix for what is very much an edge case.
		while id + amount > self.object_list_len() {
			self.grow_object_list().await?;
		}

		// Create objects
		for id in id..id + amount {
			let new_object = Object { reference_count: 1.into(), ..Default::default() };
			if let Some((mut obj, _)) = self.wait_object(id).await {
				debug_assert_eq!(
					obj.data.object().root.length(),
					0,
					"new object is not empty"
				);
				debug_assert_eq!(
					obj.data.object().reference_count,
					0,
					"new object has non-zero refcount"
				);
				obj.data.set_object(&new_object);
			} else {
				self.memory_reserve_object().await;
				let mut data = self.data.borrow_mut();
				let mut tree = TreeData::new(new_object, self.max_record_size());
				tree.set_object(&new_object); // mark as dirty.
				let busy = Busy::new(Key::new(Key::FLAG_OBJECT, id, 0, 0));
				let refcount = data.memory_tracker.finish_fetch_object(busy);
				data.objects
					.insert(id, Slot::Present(Present { data: tree, refcount }));
			}
		}

		// Tadha!
		Ok(id)
	}

	/// Get an object.
	pub async fn get(&self, id: u64) -> Result<Tree<'_, D, R>, Error<D>> {
		trace!("get {}", id);
		Ok(Tree::new(self, id))
	}

	/// Move an object to a specific ID.
	///
	/// The old object is destroyed.
	pub async fn move_object(&self, from: u64, to: u64) -> Result<(), Error<D>> {
		trace!("move_object {} -> {}", from, to);
		// Steps:
		// 1. Shrink 'to' object to zero size.
		//    Tree::shrink will move all entries to a pseudo-object.
		// 2. Reference 'to' to prevent eviction.
		// 3. Swap 'from' & 'to'.
		// 4. Set reference count of 'from' object to 0.
		// 5. Dereference 'from'.

		if from == to {
			return Ok(()); // Don't even bother.
		}

		// 1. Shrink 'to' object to zero size.
		//    Tree::shrink will move all entries to a pseudo-object.
		Tree::new(self, to).resize(0).await?;

		// 2. Reference 'to' to prevent eviction.
		let (mut obj, mut memory_tracker) = self.fetch_object(to).await?;
		memory_tracker.incr_object_refcount(&mut obj.refcount, 1);
		drop((obj, memory_tracker));

		// 3. Swap 'from' & 'to'.
		let _ = self.fetch_object(from).await?;

		let data = &mut *self.data.borrow_mut();

		let (id_min, id_max) = (from.min(to), from.max(to));
		let mut range = data.objects.range_mut(id_min..=id_max);
		let Some((_, Slot::Present(obj_min))) = range.next() else { unreachable!("no object(s)") };
		let Some((_, Slot::Present(obj_max))) = range.next_back() else { unreachable!("no object(s)") };
		let (from_obj, to_obj) = if from < to {
			(obj_min, obj_max)
		} else {
			(obj_max, obj_min)
		};

		let mut transfer = |obj: &mut Present<TreeData<R::Buf>>, new_id| {
			// Fix entries
			obj.data.transfer_entries(&mut data.memory_tracker, new_id);

			// Fix object
			let key = Key::new(Key::FLAG_OBJECT, new_id, 0, 0);
			match &obj.refcount {
				RefCount::NoRef { lru_index } => {
					*data
						.memory_tracker
						.get_key_mut(*lru_index)
						.expect("no lru entry") = key
				}
				RefCount::Ref { busy } => busy.borrow_mut().key = key,
			}

			// Mark root as dirty since it moved.
			obj.data.set_object(&obj.data.object());
		};

		transfer(from_obj, to);
		transfer(to_obj, from);
		mem::swap(from_obj, to_obj);

		// 4. Set reference count of 'from' object to 0.
		from_obj
			.data
			.set_object(&Object { reference_count: 0.into(), ..from_obj.data.object() });

		// 5. Dereference 'from'.
		data.memory_tracker
			.decr_object_refcount(&mut from_obj.refcount, 1);

		Ok(())
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
	pub fn max_record_size(&self) -> MaxRecordSize {
		self.store.max_record_size()
	}

	/// Fetch an object.
	///
	/// Specifically, fetch its root.
	async fn fetch_object<'a, 'b>(
		&'a self,

		mut id: u64,
	) -> Result<
		(
			RefMut<'a, Present<TreeData<R::Buf>>>,
			RefMut<'a, MemoryTracker>,
		),
		Error<D>,
	> {
		trace!("fetch_object {:#x}", id);
		// Steps:
		// 1. Try to get the object directly or by waiting for another tasks.
		// 2. Otherwise, fetch it ourselves.

		// 1. Try to get the object directly or by waiting for another tasks.
		if let Some(obj) = self.wait_object(id).await {
			return Ok(obj);
		}

		// 2. Otherwise, fetch it ourselves.
		debug_assert!(
			!is_pseudo_id(id),
			"can't fetch pseudo-object from object list"
		);

		// Insert a new object slot.
		let mut data = self.data.borrow_mut();
		let busy = Busy::new(Key::new(Key::FLAG_OBJECT, id, 0, 0));
		let prev = data.objects.insert(id, Slot::Busy(busy.clone()));
		debug_assert!(prev.is_none(), "object already present");
		drop(data);

		self.memory_reserve_object().await;

		// Fetch the object root.
		let object = match id {
			OBJECT_LIST_ID => Object {
				root: self.store.object_list_root(),
				total_length: self.object_list_bytelen().into(),
				reference_count: u64::MAX.into(),
				..Default::default()
			},
			OBJECT_BITMAP_ID => Object {
				root: self.store.object_bitmap_root(),
				total_length: self.object_bitmap_bytelen().into(),
				reference_count: u64::MAX.into(),
				..Default::default()
			},
			id => {
				let (offset, index) =
					util::divmod_p2(id << OBJECT_SIZE_P2, self.max_record_size().to_raw());

				let tree = Tree::new(self, OBJECT_LIST_ID);
				let fut = box_fut(tree.get(0, offset));
				let entry = fut.await?;

				let mut obj = Object::default();
				util::read(index, obj.as_mut(), entry.get());
				obj
			}
		};

		// Insert the object.
		let obj = RefMut::map_split(self.data.borrow_mut(), |data| {
			id = busy.borrow_mut().key.id();
			busy.borrow_mut().wake_all();

			let refcount = data.memory_tracker.finish_fetch_object(busy);
			let mut slot = data.objects.occupied(id).expect("no object");
			*slot.get_mut() = Slot::Present(Present {
				data: TreeData::new(object, self.max_record_size()),
				refcount,
			});

			let Slot::Present(obj) = slot.into_mut() else { unreachable!() };
			(obj, &mut data.memory_tracker)
		});

		// Presto
		Ok(obj)
	}

	/// Readjust cache size.
	///
	/// This may be useful to increase or decrease depending on total system memory usage.
	///
	/// # Panics
	///
	/// If `global_max < write_max`.
	pub fn resize_cache(&self, global_max: usize) -> Result<(), Error<D>> {
		self.data
			.borrow_mut()
			.memory_tracker
			.set_soft_limit(global_max);
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
		self.verify_cache_usage();

		let data = self.data.borrow();
		Statistics {
			storage: self.store.statistics(),
			soft_usage: data.memory_tracker.soft_usage(),
			hard_usage: data.memory_tracker.hard_usage(),
			used_objects: data
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
		self.max_record_size().to_raw() - RECORD_SIZE_P2
	}

	fn resource(&self) -> &R {
		self.store.resource()
	}

	/// The maximum amount of entries the object list can contain with its current depth.
	fn object_list_len(&self) -> u64 {
		let rec_size = self.max_record_size();
		let depth = self.store.object_list_depth();
		(1 << rec_size.to_raw() - OBJECT_SIZE_P2) * tree::max_offset(rec_size, depth)
	}

	/// The length of the object list in bytes.
	fn object_list_bytelen(&self) -> u64 {
		let rec_size = self.max_record_size();
		let depth = self.store.object_list_depth();
		(1 << rec_size.to_raw()) * tree::max_offset(rec_size, depth)
	}

	/// The length of the object bitmap in bytes.
	fn object_bitmap_bytelen(&self) -> u64 {
		self.object_list_bytelen() / OBJECT_BITMAP_FIELD_RATIO
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

fn is_pseudo_id(id: u64) -> bool {
	id != OBJECT_LIST_ID && id & ID_PSEUDO != 0
}

#[cfg(feature = "trace")]
impl<B: Buf> Drop for CacheData<B> {
	fn drop(&mut self) {
		if std::thread::panicking() {
			eprintln!("state: {:#?}", self);
		}
	}
}
