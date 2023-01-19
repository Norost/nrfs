mod entry;
mod key;
mod lru;
mod slot;
mod tree;

pub use tree::Tree;

use {
	crate::{
		resource::Buf, storage, util::box_fut, Background, BlockSize, Dev, Error, MaxRecordSize,
		Record, Resource, Store,
	},
	alloc::rc::Rc,
	core::{
		cell::{RefCell, RefMut},
		future::{self, Future},
		mem,
		num::{NonZeroU64, NonZeroUsize},
		pin::Pin,
		task::{Poll, Waker},
	},
	entry::EntryRef,
	futures_util::{stream::FuturesUnordered, TryStreamExt},
	key::Key,
	rangemap::RangeSet,
	rustc_hash::FxHashMap,
	slot::{Busy, Present, RefCount, Slot},
	std::collections::hash_map,
	tree::data::TreeData,
};

/// Fixed ID for the object list so it can use the same caching mechanisms as regular objects.
const OBJECT_LIST_ID: u64 = 1 << 59; // 2**64 / 2**5 = 2**59, ergo 2**59 is just out of range.

/// Record size as a power-of-two.
const RECORD_SIZE_P2: u8 = mem::size_of::<Record>().ilog2() as _;

/// Estimated fixed cost for every cached entry.
///
/// This is in addition to the amount of data stored by the entry.
// TODO generic... constants?
const CACHE_ENTRY_FIXED_COST: usize = 32;

const CACHE_OBJECT_FIXED_COST: usize = 128;

/// The ID refers to a pseudo-object or an entry of a pseudo-object.
pub const ID_PSEUDO: u64 = 1 << 59;

/// Cache data.
#[derive(Debug)]
pub(crate) struct CacheData<R: Resource> {
	/// Cached records of objects and the object list.
	///
	/// The key, in order, is `(id, depth, offset)`.
	/// Using separate hashmaps allows using only a prefix of the key.
	objects: FxHashMap<u64, Slot<TreeData<R>>>,
	/// LRU to manage cache size.
	lru: Lru,
	/// Used object IDs.
	used_objects_ids: RangeSet<u64>,
	/// Pseudo object ID counter.
	pseudo_id_counter: NonZeroU64,
}

impl<R: Resource> CacheData<R> {
	/// Deallocate a single object ID.
	fn dealloc_id(&mut self, id: u64) {
		debug_assert!(self.used_objects_ids.contains(&id), "double free");
		self.used_objects_ids.remove(id..id + 1);
	}

	/// Allocate a pseudo-ID.
	fn new_pseudo_id(&mut self) -> u64 {
		let mut counter @ id = self.pseudo_id_counter.get();
		counter += 1;
		counter %= 1 << 59;
		self.pseudo_id_counter = NonZeroU64::new(counter).unwrap_or(NonZeroU64::MIN);
		id | ID_PSEUDO
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

impl Lru {
	fn add(&mut self, key: Key, len: usize) -> lru::Idx {
		self.cache_size += len;
		self.lru.insert(key)
	}

	fn remove(&mut self, index: lru::Idx, len: usize) {
		self.lru.remove(index);
		self.cache_size -= len;
	}

	/// Decrease reference count.
	///
	/// Inserts a new entry if it reaches 0.
	///
	/// # Panics
	///
	/// If the reference count is [`RefCount::NoRef`].
	fn decrease_refcount(&mut self, refcount: &mut RefCount, key: Key, len: usize) {
		let RefCount::Ref { count } = refcount else { panic!("NoRef") };
		match NonZeroUsize::new(count.get() - 1) {
			Some(c) => *count = c,
			None => {
				let lru_index = self.add(key, len);
				*refcount = RefCount::NoRef { lru_index };
			}
		}
	}

	/// Increase reference count.
	///
	/// Removes entry if it was 0.
	///
	/// # Panics
	///
	/// If the reference count overflows.
	fn increase_refcount(&mut self, refcount: &mut RefCount, len: usize) {
		match refcount {
			RefCount::Ref { count } => *count = count.checked_add(1).unwrap(),
			RefCount::NoRef { lru_index } => {
				self.remove(*lru_index, len);
				*refcount = RefCount::Ref { count: NonZeroUsize::new(1).unwrap() };
			}
		}
	}
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
				objects: Default::default(),
				lru: Lru { lru: Default::default(), cache_max: global_cache_max, cache_size: 0 },
				used_objects_ids,
				pseudo_id_counter: NonZeroU64::MIN,
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

	/// Create an object.
	pub async fn create<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
	) -> Result<Tree<'a, 'b, D, R>, Error<D>> {
		trace!("create");
		let id = self.create_many::<1>(bg).await?;
		Ok(Tree::new(self, bg, id))
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
		let offset = id << RECORD_SIZE_P2;

		// Write
		let tree = Tree::new(self, bg, OBJECT_LIST_ID);
		let len = self
			.data
			.borrow_mut()
			.used_objects_ids
			.iter()
			.last()
			.map_or(0, |r| r.end);
		let len = len.max(id + N as u64);
		tree.resize(len << RECORD_SIZE_P2).await?;
		tree.write(offset, b.flatten()).await?;

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
		Ok(Tree::new(self, bg, id))
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
		// Steps:
		// 1. Move 'to' object to pseudo ID.
		// 2. Destroy object at pseudo ID.
		// 3. Move 'from' object to 'to' ID.
		// 4. Clear 'from' in object list.

		if from == to {
			return Ok(()); // Don't even bother.
		}

		// Helper function to fixup keys
		let transfer = |from, to| {
			let mut data = self.data.borrow_mut();
			let Some(Slot::Present(obj)) = data.objects.remove(&from) else { panic!("no object") };

			for level in obj.data.data.iter() {
				for slot in level.slots.values() {
					let Slot::Present(Present { refcount: RefCount::NoRef { lru_index }, .. }) = slot
						else { continue };
					let key = data.lru.lru.get_mut(*lru_index).expect("not in lru");
					*key = Key::new(0, to, key.depth(), key.offset());
				}
			}

			data.objects.insert(to, Slot::Present(obj));
		};

		// FIXME tasks may still be using the old IDs.

		// 1. Move 'to' object to pseudo ID.
		let _ = self.fetch_object(bg, to).await?;
		let pseudo_id = self.data.borrow_mut().new_pseudo_id();
		transfer(to, pseudo_id);

		// 2. Destroy object at pseudo ID.
		Tree::new(self, bg, pseudo_id)
			.write_zeros(0, u64::MAX)
			.await?;

		// 3. Move 'from' object to 'to' ID.
		let _ = self.fetch_object(bg, from).await?;
		transfer(from, to);

		// 4. Clear 'from' in object list.
		let offset = from << RECORD_SIZE_P2;
		Tree::new(self, bg, OBJECT_LIST_ID)
			.write_zeros(offset, 1 << RECORD_SIZE_P2)
			.await?;
		self.data
			.borrow_mut()
			.used_objects_ids
			.remove(from..from + 1);

		Ok(())
	}

	/// Finish the current transaction, committing any changes to the underlying devices.
	pub async fn finish_transaction<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
	) -> Result<(), Error<D>> {
		// First flush cache
		self.flush_all(bg).await?;

		// Ensure all data has been written.
		bg.try_run_all().await?;

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

	/// Try to get an object directly.
	fn get_object(&self, id: u64) -> Option<RefMut<'_, TreeData<R>>> {
		RefMut::filter_map(self.data.borrow_mut(), |d| match d.objects.get_mut(&id) {
			Some(Slot::Present(slot)) => Some(&mut slot.data),
			_ => None,
		})
		.ok()
	}

	/// Fetch an object.
	///
	/// Specifically, fetch its root.
	async fn fetch_object<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		id: u64,
	) -> Result<(RefMut<'a, Present<TreeData<R>>>, RefMut<'a, Lru>), Error<D>> {
		trace!("fetch_object {:?}", id);
		// Steps:
		// 1. Check the state of the object.
		// 1a. If present, just return.
		// 1b. If busy, wait, then check 1a and 1b.
		// 1c. If not present, fetch the root.

		let key = Key::new(Key::FLAG_OBJECT, id, 0, 0);

		// Try to get the object directly, i.e. without fetching it ourselves.
		let mut is_referenced = false;
		let obj = future::poll_fn(|cx| {
			let mut must_fetch = true;
			let (objects, mut lru) =
				RefMut::map_split(self.data.borrow_mut(), |d| (&mut d.objects, &mut d.lru));
			let ret = RefMut::filter_map(objects, |objects| {
				// 1. Check the state of the object.
				match objects.entry(id) {
					// 1c. If not present, fetch the root.
					hash_map::Entry::Vacant(slot) => {
						// We need to fetch it ourselves.
						slot.insert(Slot::Busy(Busy::new(key)));
						must_fetch = true;
						None
					}
					hash_map::Entry::Occupied(mut slot) => match slot.into_mut() {
						// 1a. If present, just return.
						Slot::Present(obj) => {
							if is_referenced {
								lru.decrease_refcount(
									&mut obj.refcount,
									key,
									CACHE_OBJECT_FIXED_COST,
								);
							}
							Some(obj)
						}
						// 1b. If busy, wait, then check 1a and 1b.
						Slot::Busy(busy) => {
							let mut busy = busy.borrow_mut();
							if !is_referenced {
								busy.refcount =
									NonZeroUsize::new(busy.refcount.map_or(0, |x| x.get()) + 1);
								is_referenced = true;
							}
							busy.wakers.push(cx.waker().clone());
							None
						}
					},
				}
			});
			match ret {
				Ok(obj) => Poll::Ready(Some((obj, lru))),
				Err(_) if must_fetch => Poll::Ready(None),
				Err(_) => Poll::Pending,
			}
		})
		.await;

		if let Some(obj) = obj {
			return Ok(obj);
		}

		// 1c. If not present, fetch the root.

		// Fetch the object root.
		let mut root = Record::default();
		if id < OBJECT_LIST_ID {
			let tree = Tree::new(self, bg, OBJECT_LIST_ID);
			let fut = box_fut(tree.read(id << RECORD_SIZE_P2, root.as_mut()));
			let len = fut.await?;
			assert_eq!(len, 1 << RECORD_SIZE_P2, "read partial root");
		} else {
			root = self.store.object_list();
		}

		// Insert the entry.
		let obj = RefMut::map_split(self.data.borrow_mut(), |data| {
			let slot = data.objects.get_mut(&id).expect("no object");
			let Slot::Busy(busy) = slot else { unreachable!("not busy") };
			let mut busy = busy.borrow_mut();
			busy.wakers.drain(..).for_each(|w| w.wake());
			let refcount = RefCount::busy_to_present(
				busy.refcount,
				&mut data.lru,
				Key::new(Key::FLAG_OBJECT, id, 0, 0),
				CACHE_OBJECT_FIXED_COST,
			);
			drop(busy);
			*slot = Slot::Present(Present {
				data: TreeData::new(root, self.max_record_size()),
				refcount,
			});
			let Slot::Present(obj) = slot else { unreachable!() };
			(obj, &mut data.lru)
		});

		// Presto
		Ok(obj)
	}

	/// Fetch a record for a cache entry.
	///
	/// If the entry is already being fetched,
	/// the caller is instead added to a list to be waken up when the fetcher has finished.
	async fn fetch_entry<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		key: Key,
		record: &Record,
	) -> Result<EntryRef<'a, D, R>, Error<D>> {
		trace!("fetch_entry {:?} <- {:?}", key, record.lba);
		// Steps:
		// 1. Fetch the corresponding object first.
		// 2. Check the state of the entry.
		// 2a. If present, just return.
		// 2b. If busy, wait, then check 2a and 2b.
		// 2c. If not present, fetch the entry.

		// Check for use-after-frees.
		#[cfg(debug_assertions)]
		self.store.assert_alloc(record);

		// 1. Fetch the corresponding object first.
		let (mut obj, mut lru) = self.fetch_object(bg, key.id()).await?;

		// 2. Check the state of the entry.
		if obj.data.data[usize::from(key.depth())]
			.slots
			.contains_key(&key.offset())
		{
			drop((obj, lru));
			let mut is_referenced = false;
			let entry = future::poll_fn(|cx| {
				let (objects, mut lru) =
					RefMut::map_split(self.data.borrow_mut(), |d| (&mut d.objects, &mut d.lru));
				let ret = RefMut::filter_map(objects, |objects| {
					let Some(Slot::Present(obj)) = objects.get_mut(&key.id())
						else { unreachable!("no object") };
					let slot = obj.data.data[usize::from(key.depth())]
						.slots
						.get_mut(&key.offset())
						.expect("no entry");
					match slot {
						// 2a. If present, just return.
						Slot::Present(entry) => {
							if is_referenced {
								lru.decrease_refcount(
									&mut entry.refcount,
									key,
									CACHE_ENTRY_FIXED_COST + entry.data.len(),
								);
							}
							Some(entry)
						}
						// 2b. If busy, wait, then check 2a and 2b.
						Slot::Busy(busy) => {
							let mut busy = busy.borrow_mut();
							if !is_referenced {
								busy.refcount =
									NonZeroUsize::new(busy.refcount.map_or(0, |x| x.get()) + 1);
								is_referenced = true;
							}
							busy.wakers.push(cx.waker().clone());
							None
						}
					}
				});
				match ret {
					Ok(entry) => Poll::Ready(EntryRef::new(self, key, entry, lru)),
					Err(_) => Poll::Pending,
				}
			})
			.await;
			return Ok(entry);
		}

		// 2c. If not present, fetch the entry.

		// Insert a new entry and increase refcount to object.
		obj.add_entry(
			&mut lru,
			key.depth(),
			key.offset(),
			Slot::Busy(Busy::new(key)),
		);
		drop((obj, lru));

		// Fetch it
		let entry = self.store.read(record).await?;

		// Insert the entry.
		let (entry, lru) = RefMut::map_split(self.data.borrow_mut(), |data| {
			let Some(Slot::Present(obj)) = data.objects.get_mut(&key.id())
				else { unreachable!("no object") };
			let slot = obj.data.data[usize::from(key.depth())]
				.slots
				.get_mut(&key.offset())
				.expect("no entry");
			let Slot::Busy(busy) = slot else { unreachable!("not busy") };
			let mut busy = busy.borrow_mut();
			busy.wakers.drain(..).for_each(|w| w.wake());
			let refcount = RefCount::busy_to_present(
				busy.refcount,
				&mut data.lru,
				key,
				CACHE_ENTRY_FIXED_COST + entry.len(),
			);
			drop(busy);
			*slot = Slot::Present(Present { data: entry, refcount });
			let Slot::Present(e) = slot else { unreachable!() };
			(e, &mut data.lru)
		});

		// Presto
		Ok(EntryRef::new(self, key, entry, lru))
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
		self.data.borrow_mut().lru.cache_max = global_max;
		self.evict_excess(bg);
		Ok(())
	}

	/// Evict entries if global cache limits are being exceeded.
	fn evict_excess<'a, 'b>(&'a self, bg: &'b Background<'a, D>) {
		trace!("evict_excess");
		let mut data = self.data.borrow_mut();

		while data.lru.cache_size > data.lru.cache_max {
			// Get last read entry.
			let &key = data
				.lru
				.lru
				.last()
				.expect("no nodes despite non-zero write cache size");

			// Push to background tasks queue to process in parallel
			drop(data);
			if let Some(task) = self.evict_entry(key) {
				bg.run_background(task);
			}
			data = self.data.borrow_mut();
		}
	}

	/// Evict an entry from the cache.
	///
	/// Does nothing if the entry wasn't present.
	fn evict_entry(
		&self,
		key: Key,
	) -> Option<Pin<Box<dyn Future<Output = Result<(), Error<D>>> + '_>>> {
		trace!("evict_entry {:?}", key);

		// Get object
		let data = &mut *self.data.borrow_mut();
		let Some(Slot::Present(obj)) = data.objects.get_mut(&key.id())
			else { unreachable!("no object") };

		if key.test_flag(Key::FLAG_OBJECT) {
			// Evict object

			let mut root = obj.data.root;
			root._reserved = 0;

			let fut = if key.id() == OBJECT_LIST_ID {
				self.store.set_object_list(root);
				None
			} else {
				debug_assert_eq!(
					key.id() & ID_PSEUDO,
					0,
					"pseudo object should not be in the LRU"
				);

				obj.data.is_dirty().then(|| {
					let offset = key.id() << RECORD_SIZE_P2;
					async move {
						let bg = Default::default(); // TODO get rid of this sillyness
						Tree::new(self, &bg, OBJECT_LIST_ID)
							.write(offset, root.as_ref())
							.await?;
						bg.drop().await
					}
				})
			};

			let RefCount::NoRef { lru_index } = obj.refcount
				else { unreachable!("not in lru") };
			data.lru.remove(lru_index, CACHE_OBJECT_FIXED_COST);
			data.objects.remove(&key.id());

			fut.map(box_fut)
		} else {
			// Evict entry

			// Check if the entry is dirty.
			let entry = if obj
				.data
				.unmark_dirty(key.depth(), key.offset(), self.max_record_size())
			{
				let level = &mut obj.data.data[usize::from(key.depth())];

				let slot = level.slots.get_mut(&key.offset()).expect("no entry");
				let Slot::Present(Present { data: entry, refcount: RefCount::NoRef { lru_index } }) = slot
					else { unreachable!("no entry, or entry not in LRU") };
				let entry = mem::replace(entry, self.resource().alloc());

				data.lru
					.remove(*lru_index, entry.len() + CACHE_ENTRY_FIXED_COST);

				let busy = Rc::new(RefCell::new(Busy { wakers: vec![], refcount: None, key }));
				*slot = Slot::Busy(busy.clone());

				Some((entry, busy))
			} else {
				// Just remove the entry.
				let level = &mut obj.data.data[usize::from(key.depth())];
				let Some(Slot::Present(Present { data: entry, refcount: RefCount::NoRef { lru_index } })) = level.slots.remove(&key.offset())
					else { unreachable!("no entry") };
				data.lru
					.remove(lru_index, entry.len() + CACHE_ENTRY_FIXED_COST);
				// Dereference the corresponding object.
				let flags = Key::FLAG_OBJECT | key.flags();
				data.lru.decrease_refcount(
					&mut obj.refcount,
					Key::new(flags, key.id(), key.depth(), key.offset()),
					CACHE_OBJECT_FIXED_COST,
				);
				None
			};

			let entry = entry.map(|(entry, busy)| {
				async move {
					trace!("evict_entry::is_dirty {:?}", key);
					// Store record.
					let (record, entry) = self.store.write(entry).await?;

					let key = busy.borrow_mut().key;

					let bg = Default::default(); // TODO get rid of this sillyness
					Tree::new(self, &bg, key.id())
						.update_record(key.depth(), key.offset(), record)
						.await?;

					// Unmark as being flushed.
					let mut data_ref = self.data.borrow_mut();
					let data = &mut *data_ref;
					let Some(Slot::Present(obj)) = data.objects.get_mut(&key.id())
						else { unreachable!("no object") };
					let level = &mut obj.data.data[usize::from(key.depth())];
					let hash_map::Entry::Occupied(mut slot) = level.slots.entry(key.offset())
						else { unreachable!("no entry") };

					let Slot::Busy(busy) = slot.get_mut()
						else { unreachable!("no entry or entry is not flushing") };
					let mut busy = busy.borrow_mut();

					// Wake a waiting fetcher, if any.
					// We have to insert the entry again since the fetcher *will* have an outdated record.
					if let Some(count) = busy.refcount.take() {
						debug_assert!(
							!busy.wakers.is_empty(),
							"refcount is non-zero without wakers"
						);
						busy.wakers.drain(..).for_each(|w| w.wake());
						drop(busy);
						*slot.get_mut() = Slot::Present(Present {
							data: entry,
							refcount: RefCount::Ref { count },
						});
					} else {
						debug_assert!(busy.wakers.is_empty(), "refcount is zero with wakers");
						drop(busy);
						slot.remove();
						// Dereference the corresponding object.
						let flags = Key::FLAG_OBJECT | key.flags();
						data.lru.decrease_refcount(
							&mut obj.refcount,
							Key::new(flags, key.id(), key.depth(), key.offset()),
							CACHE_OBJECT_FIXED_COST,
						);
					}

					drop(data_ref);
					// Make sure we only drop the "background" runner at the end to avoid
					// getting stuck when something tries to fetch the entry that is
					// being evicted.
					//
					// Specifically, we must ensure the Flushing state is removed before
					// we attempt to run the background tasks to completion.
					bg.drop().await
				}
			});
			entry.map(box_fut)
		}
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
		let mut data_ref = self.data.borrow_mut();
		let data = &mut *data_ref;
		// Clear dirty status
		let Some(Slot::Present(obj)) = data.objects.get_mut(&key.id()) else { return Ok(()) };
		obj.data
			.unmark_dirty(key.depth(), key.offset(), self.max_record_size());

		// Take entry.
		let level = &mut obj.data.data[usize::from(key.depth())];
		let slot = level.slots.get_mut(&key.offset()).expect("no entry");
		let Slot::Present(present) = slot else { unreachable!("no entry") };
		let entry = mem::replace(&mut present.data, self.resource().alloc());
		let refcount = present
			.refcount
			.present_to_busy(&mut data.lru, CACHE_ENTRY_FIXED_COST + entry.len());
		*slot = Slot::Busy(Busy::with_refcount(key, refcount));

		// Store entry.
		drop(data_ref);
		let (rec, entry) = self.store.write(entry).await?;
		Tree::new(self, bg, key.id())
			.update_record(key.depth(), key.offset(), rec)
			.await?;

		// Put entry back in.
		let data = &mut *self.data.borrow_mut();
		let Some(Slot::Present(obj)) = data.objects.get_mut(&key.id())
			else { unreachable!("no object") };
		let level = &mut obj.data.data[usize::from(key.depth())];
		let slot = level.slots.get_mut(&key.offset()).expect("no entry");
		let Slot::Busy(busy) = slot else { unreachable!("entry not busy") };
		let mut busy = busy.borrow_mut();
		busy.wakers.drain(..).for_each(|w| w.wake());
		let refcount = RefCount::busy_to_present(
			busy.refcount,
			&mut data.lru,
			key,
			CACHE_ENTRY_FIXED_COST + entry.len(),
		);
		drop(busy);
		*slot = Slot::Present(Present { data: entry, refcount });

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
			.objects
			.keys()
			.copied()
			.filter(|&id| id != OBJECT_LIST_ID)
			.collect::<Vec<_>>();
		for depth in 0..16 {
			for &id in ids.iter() {
				let Some(tree) = data.objects.get_mut(&id) else { continue };
				let Slot::Present(tree) = tree else { unreachable!() };
				let Some(level) = tree.data.data.get_mut(usize::from(depth)) else { continue };
				let offsets = level.dirty_markers.keys().copied().collect::<Vec<_>>();
				drop(data);

				// Flush in parallel.
				for offt in offsets {
					let key = Key::new(0, id, depth, offt);
					queue.push(self.flush_entry(bg, key));
				}
				while queue.try_next().await?.is_some() {}

				// Wait for background tasks in case higher records got flushed.
				bg.try_run_all().await?;

				data = self.data.borrow_mut();
			}
		}

		// Write object roots.
		// Sort the objects to take better advantage of caching.
		let mut ids = data
			.objects
			.iter()
			.filter(|&(&id, obj)| {
				let Slot::Present(obj) = obj else { unreachable!() };
				id != OBJECT_LIST_ID && obj.data.is_dirty()
			})
			.map(|(id, _)| *id)
			.collect::<Vec<_>>();
		ids.sort_unstable();
		for id in ids {
			let Some(Slot::Present(obj)) = data.objects.get(&id) else { continue };
			let mut root = obj.data.root;
			root._reserved = 0;
			drop(data);
			let offset = id << RECORD_SIZE_P2;
			Tree::new(self, bg, OBJECT_LIST_ID)
				.write(offset, root.as_ref())
				.await?;
			data = self.data.borrow_mut();
		}

		// Now flush the object list.
		for depth in 0..16 {
			let Some(tree) = data.objects.get_mut(&OBJECT_LIST_ID) else { continue };
			let Slot::Present(tree) = tree else { unreachable!() };
			let Some(level) = tree.data.data.get_mut(usize::from(depth)) else { continue };
			let offsets = level.dirty_markers.keys().copied().collect::<Vec<_>>();
			drop(data);

			// Flush in parallel.
			for offt in offsets {
				let key = Key::new(0, OBJECT_LIST_ID, depth, offt);
				queue.push(self.flush_entry(bg, key));
			}
			while queue.try_next().await?.is_some() {}

			// Wait for background tasks in case higher records got flushed.
			bg.try_run_all().await?;

			data = self.data.borrow_mut();
		}

		// Write object list root.
		if let Some(Slot::Present(obj)) = data.objects.get(&OBJECT_LIST_ID) {
			let mut root = obj.data.root;
			root._reserved = 0;
			self.store.set_object_list(root);
		}

		// Tadha!
		// Do a sanity check just in case.
		if cfg!(debug_assertions) {
			for tree in data.objects.values() {
				let Slot::Present(tree) = tree else { unreachable!() };
				for level in tree.data.data.iter() {
					debug_assert!(level.dirty_markers.is_empty(), "flush_all didn't flush all");
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
		Statistics { storage: self.store.statistics(), global_usage: data.lru.cache_size }
	}

	/// Check if cache size matches real usage
	#[cfg(test)]
	#[track_caller]
	pub fn verify_cache_usage(&self) {
		let data = self.data.borrow();
		let real_usage = data.objects.values().fold(0, |x, s| {
			x + if let Slot::Present(slot) = s {
				let mut y = 0;
				if matches!(slot.refcount, RefCount::NoRef { .. }) {
					y += CACHE_OBJECT_FIXED_COST;
				}
				y += slot
					.data
					.data
					.iter()
					.flat_map(|m| m.slots.values())
					.flat_map(|s| match s {
						Slot::Present(slot) => Some(&slot.data),
						_ => None,
					})
					.fold(0, |x, v| x + v.len() + CACHE_ENTRY_FIXED_COST);
				y
			} else {
				0
			}
		});
		assert_eq!(real_usage, data.lru.cache_size, "cache size mismatch");
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
