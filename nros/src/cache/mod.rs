mod entry;
mod key;
mod lru;
mod object;
mod slot;
mod tree;

pub use tree::Tree;

use {
	crate::{
		resource::Buf, storage, util::box_fut, Background, BlockSize, Dev, Error, MaxRecordSize,
		Record, Resource, Store,
	},
	core::{
		cell::{RefCell, RefMut},
		future::Future,
		mem,
		num::NonZeroU64,
		pin::Pin,
	},
	entry::EntryRef,
	futures_util::{stream::FuturesUnordered, TryStreamExt},
	key::Key,
	lru::Lru,
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
		trace!("new_pseudo_id");
		let mut counter @ id = self.pseudo_id_counter.get();
		counter += 1;
		counter %= 1 << 59;
		self.pseudo_id_counter = NonZeroU64::new(counter).unwrap_or(NonZeroU64::MIN);
		trace!("--> {:#x}", id | ID_PSEUDO);
		id | ID_PSEUDO
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
				lru: Lru::new(global_cache_max),
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
		let id = self.create_many(bg, 1).await?;
		Ok(Tree::new(self, bg, id))
	}

	/// Create many adjacent objects.
	pub async fn create_many<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		amount: u64,
	) -> Result<u64, Error<D>> {
		trace!("create_many {}", amount);
		// Allocate
		let id = self.alloc_ids(amount);
		{
			trace!("--> {}", id);
		}

		// Resize if necessary
		let len = self
			.data
			.borrow_mut()
			.used_objects_ids
			.iter()
			.last()
			.map_or(0, |r| r.end);
		Tree::new(self, bg, OBJECT_LIST_ID)
			.resize(len << RECORD_SIZE_P2)
			.await?;

		// Create objects
		for id in id..id + amount {
			let new_root = Record { references: 1.into(), ..Default::default() };
			if let Some((mut obj, _)) = self.wait_object(id).await {
				debug_assert_eq!(obj.data.root().length, 0, "new object is not empty");
				debug_assert_eq!(
					obj.data.root().references,
					0,
					"new object has non-zero refcount"
				);
				obj.data.set_root(&new_root);
			} else {
				let mut data = self.data.borrow_mut();
				let mut tree = TreeData::new(new_root, self.max_record_size());
				tree.set_root(&new_root); // mark as dirty.
				let tree = Present { data: tree, refcount: data.lru.object_add_noref(id) };
				data.objects.insert(id, Slot::Present(tree));
			}
		}

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
		Tree::new(self, bg, to).resize(0).await?;

		// 2. Reference 'to' to prevent eviction.
		let (mut obj, mut lru) = self.fetch_object(bg, to).await?;
		lru.object_increase_refcount(&mut obj.refcount);
		drop((obj, lru));

		// 3. Swap 'from' & 'to'.
		let _ = self.fetch_object(bg, from).await?;

		let data = &mut *self.data.borrow_mut();

		let Some([Slot::Present(from_obj), Slot::Present(to_obj)]) = data.objects.get_many_mut([&from, &to])
			else { unreachable!("no object(s)") };

		let mut transfer = |obj: &mut Present<TreeData<R>>, new_id| {
			// Fix entries
			for level in obj.data.data.iter() {
				for slot in level.slots.values() {
					let f = |key: Key| Key::new(0, new_id, key.depth(), key.offset());
					match slot {
						Slot::Present(Present {
							refcount: RefCount::NoRef { lru_index }, ..
						}) => {
							let key = data.lru.get_mut(*lru_index).expect("not in lru");
							*key = f(*key)
						}
						Slot::Present(Present { refcount: RefCount::Ref { busy }, .. })
						| Slot::Busy(busy) => {
							let mut busy = busy.borrow_mut();
							busy.key = f(busy.key);
						}
					}
				}
			}

			// Fix object
			let key = Key::new(Key::FLAG_OBJECT, new_id, 0, 0);
			match &obj.refcount {
				RefCount::NoRef { lru_index } => {
					*data.lru.get_mut(*lru_index).expect("no lru entry") = key
				}
				RefCount::Ref { busy } => busy.borrow_mut().key = key,
			}

			// Mark root as dirty since it moved.
			obj.data.set_root(&obj.data.root());
		};

		transfer(from_obj, to);
		transfer(to_obj, from);
		mem::swap(from_obj, to_obj);

		// 4. Set reference count of 'from' object to 0.
		from_obj
			.data
			.set_root(&Record { references: 0.into(), ..from_obj.data.root() });

		// 5. Dereference 'from'.
		data.lru
			.object_decrease_refcount(from, &mut from_obj.refcount, 1);

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
		mut id: u64,
	) -> Result<(RefMut<'a, Present<TreeData<R>>>, RefMut<'a, Lru>), Error<D>> {
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

		// Insert the object.
		let obj = RefMut::map_split(self.data.borrow_mut(), |data| {
			let mut busy_ref = busy.borrow_mut();
			id = busy_ref.key.id();
			busy_ref.wakers.drain(..).for_each(|w| w.wake());
			drop(busy_ref);
			let refcount = data.lru.object_add(id, busy);

			let hash_map::Entry::Occupied(mut slot) = data.objects.entry(id)
				else { unreachable!("not busy") };
			*slot.get_mut() = Slot::Present(Present {
				data: TreeData::new(root, self.max_record_size()),
				refcount,
			});

			let Slot::Present(obj) = slot.into_mut() else { unreachable!() };
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
		trace!(
			"fetch_entry {:?} <- ({}, {})",
			key,
			record.lba,
			record.length
		);
		// Steps:
		// 1. Try to get the entry directly or by waiting for another tasks.
		// 2. Otherwise, fetch it ourselves.

		// 1. Try to get the entry directly or by waiting for another tasks.
		if let Some(entry) = self.wait_entry(key).await {
			return Ok(entry);
		}

		// 2. Otherwise, fetch it ourselves.

		// Insert a new entry and increase refcount to object.
		let (mut obj, mut lru) = self.fetch_object(bg, key.id()).await?;
		let busy = Busy::new(key);
		obj.add_entry(
			&mut lru,
			key.depth(),
			key.offset(),
			Slot::Busy(busy.clone()),
		);
		drop((obj, lru));

		// Fetch it
		let entry = self.store.read(record).await?;

		let key = busy.borrow_mut().key;

		// Insert the entry.
		let mut comp = self.get_entryref_components(key).expect("no entry");
		let entry = RefMut::map(comp.slot, |slot| {
			let Slot::Busy(busy) = slot else { unreachable!("not busy") };
			busy.borrow_mut().wakers.drain(..).for_each(|w| w.wake());
			let refcount = comp.lru.entry_add(key, busy.clone(), entry.len());

			*slot = Slot::Present(Present { data: entry, refcount });
			let Slot::Present(e) = slot else { unreachable!() };
			e
		});

		// Presto
		Ok(EntryRef::new(
			self,
			key,
			entry,
			comp.dirty_markers,
			comp.lru,
		))
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
		self.data.borrow_mut().lru.set_cache_max(global_max);
		self.evict_excess(bg);
		Ok(())
	}

	/// Evict entries if global cache limits are being exceeded.
	fn evict_excess<'a, 'b>(&'a self, bg: &'b Background<'a, D>) {
		trace!("evict_excess");
		let mut data = self.data.borrow_mut();

		while data.lru.has_excess() {
			// Get last read entry.
			let key = data
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

			// Take root
			let root = obj.data.root();

			// Remove from LRU
			let RefCount::NoRef { lru_index } = obj.refcount
				else { unreachable!("not in lru") };
			data.lru.object_remove(lru_index);

			let fut = if key.id() == OBJECT_LIST_ID {
				// Just copy
				trace!("--> object list");
				self.store.set_object_list(root);
				data.objects.remove(&key.id());
				None
			} else if obj.data.is_dirty() {
				// Save the root
				debug_assert!(
					key.id() & ID_PSEUDO == 0,
					"pseudo object (id: {:#x}) should not be in the LRU",
					key.id()
				);

				let busy = Busy::new(key);
				let Some(Slot::Present(mut obj)) = data.objects.insert(key.id(), Slot::Busy(busy.clone()))
					else { unreachable!("no object") };

				let offset = key.id() << RECORD_SIZE_P2;

				Some(async move {
					trace!("evict_entry::object {:#x}", key.id());

					let bg = Background::default(); // TODO get rid of this sillyness
					bg.run(async {
						Tree::new(self, &bg, OBJECT_LIST_ID)
							.write(offset, root.as_ref())
							.await?;

						let mut busy_ref = busy.borrow_mut();
						debug_assert_eq!(
							busy_ref.key.id(),
							key.id(),
							"id of object changed while evicting"
						);

						let mut data = self.data.borrow_mut();
						let hash_map::Entry::Occupied(mut slot) = data.objects.entry(key.id())
							else { unreachable!("no object") };

						if busy_ref.refcount > 0 {
							debug_assert!(
								!busy_ref.wakers.is_empty(),
								"non-zero refcount with no wakers"
							);
							busy_ref.wakers.drain(..).for_each(|w| w.wake());
							drop(busy_ref);
							obj.refcount = RefCount::Ref { busy };
							slot.insert(Slot::Present(obj));
						} else {
							debug_assert!(busy_ref.wakers.is_empty(), "zero refcount with wakers");
							drop(busy_ref);
							slot.remove();
						}

						Ok::<_, Error<D>>(())
					})
					.await?;
					bg.drop().await
				})
			} else {
				// Just remove
				trace!("--> not dirty");
				data.objects.remove(&key.id());
				None
			};

			fut.map(box_fut)
		} else {
			// Evict entry

			// Check if the entry is dirty.
			let entry = if obj.data.is_marked_dirty(key.depth(), key.offset()) {
				let level = &mut obj.data.data[usize::from(key.depth())];

				let slot = level.slots.get_mut(&key.offset()).expect("no entry");
				let Slot::Present(Present { data: entry, refcount: RefCount::NoRef { lru_index } }) = slot
					else { unreachable!("no entry, or entry not in LRU") };
				let entry = mem::replace(entry, self.resource().alloc());

				data.lru.entry_remove(*lru_index, entry.len());

				let busy = Busy::new(key);
				*slot = Slot::Busy(busy.clone());

				Some((entry, busy))
			} else {
				// Just remove the entry.
				trace!("--> not dirty");
				let level = &mut obj.data.data[usize::from(key.depth())];
				let Some(Slot::Present(Present { data: entry, refcount: RefCount::NoRef { lru_index } })) = level.slots.remove(&key.offset())
					else { unreachable!("no entry") };
				data.lru.entry_remove(lru_index, entry.len());
				// Dereference the corresponding object.
				if data
					.lru
					.object_decrease_refcount(key.id(), &mut obj.refcount, 1)
				{
					data.objects.remove(&key.id());
				}
				None
			};

			let entry = entry.map(|(entry, busy)| {
				async move {
					trace!("evict_entry::entry {:?}", key);
					// Store record.
					let (record, entry) = self.store.write(entry).await?;

					{
						trace!("{:?} ~1> {:?}", key, busy.borrow_mut().key);
					}
					let key = busy.borrow_mut().key;

					let bg = Background::default(); // TODO get rid of this sillyness
					bg.run(Tree::new(self, &bg, key.id()).update_record(
						key.depth(),
						key.offset(),
						record,
					))
					.await?;

					{
						trace!("{:?} ~2> {:?}", key, busy.borrow_mut().key);
					}
					let key = busy.borrow_mut().key;

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
					let mut busy_ref = busy.borrow_mut();

					// Wake a waiting fetcher, if any.
					// We have to insert the entry again since the fetcher *will* have an outdated record.
					let mut do_remove = false;
					if busy_ref.refcount > 0 {
						debug_assert!(
							!busy_ref.wakers.is_empty(),
							"refcount is non-zero without wakers"
						);
						busy_ref.wakers.drain(..).for_each(|w| w.wake());
						drop(busy_ref);
						*slot.get_mut() = Slot::Present(Present {
							data: entry,
							refcount: RefCount::Ref { busy: busy.clone() },
						});
					} else {
						debug_assert!(busy_ref.wakers.is_empty(), "refcount is zero with wakers");
						drop(busy_ref);
						slot.remove();
						do_remove =
							data.lru
								.object_decrease_refcount(key.id(), &mut obj.refcount, 1);
					}

					// Remove dirty marker
					let _r =
						obj.data
							.unmark_dirty(key.depth(), key.offset(), self.max_record_size());
					debug_assert!(_r, "not marked");

					if do_remove {
						// Forget the object, which should be all zeroes.
						debug_assert!(obj.data.root().length == 0, "pseudo object leaks entries");
						data.objects.remove(&key.id());
					}

					drop(data_ref);
					// Make sure we only drop the "background" runner at the end to avoid
					// getting stuck when something tries to fetch the entry that is
					// being evicted.
					//
					// Specifically, we must ensure the Flushing state is removed before
					// we attempt to run the background tasks to completion.
					bg.drop().await?;
					Ok(())
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

		if !obj.data.is_marked_dirty(key.depth(), key.offset()) {
			// The entry is not dirty, so skip.
			return Ok(());
		}

		// Take entry.
		let level = &mut obj.data.data[usize::from(key.depth())];

		let slot = level.slots.get_mut(&key.offset()).expect("no entry");
		let Slot::Present(present) = slot else {
			// The entry is already being evicted - probably
			// FIXME we need a better guarantee than this.
			return Ok(());
		};

		let entry = mem::replace(&mut present.data, self.resource().alloc());
		let busy = match &present.refcount {
			RefCount::Ref { busy } => busy.clone(),
			RefCount::NoRef { lru_index } => {
				data.lru.entry_remove(*lru_index, entry.len());
				Busy::new(key)
			}
		};
		*slot = Slot::Busy(busy);

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
		busy.borrow_mut().wakers.drain(..).for_each(|w| w.wake());
		let refcount = data.lru.entry_add(key, busy.clone(), entry.len());

		*slot = Slot::Present(Present { data: entry, refcount });

		// Unmark as dirty
		let r = obj
			.data
			.unmark_dirty(key.depth(), key.offset(), self.max_record_size());
		debug_assert!(r, "not marked");

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
				id & ID_PSEUDO == 0 && obj.data.is_dirty()
			})
			.map(|(id, _)| *id)
			.collect::<Vec<_>>();

		ids.sort_unstable();

		for id in ids {
			let Some(Slot::Present(obj)) = data.objects.get_mut(&id) else { continue };
			debug_assert!(!is_pseudo_id(id), "can't flush pseudo ID");
			let root = obj.data.root();
			obj.data.clear_dirty();
			drop(data);
			let offset = id << RECORD_SIZE_P2;
			Tree::new(self, bg, OBJECT_LIST_ID)
				.write(offset, root.as_ref())
				.await?;
			data = self.data.borrow_mut();
		}

		drop(data);
		bg.try_run_all().await?;
		data = self.data.borrow_mut();

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
			self.store.set_object_list(obj.data.root());
		}

		// Tadha!
		// Do a sanity check just in case.
		if cfg!(debug_assertions) {
			for (&id, tree) in data.objects.iter() {
				let Slot::Present(tree) = tree else { unreachable!() };

				for level in tree.data.data.iter() {
					debug_assert!(level.dirty_markers.is_empty(), "flush_all didn't flush all");
				}

				if id != OBJECT_LIST_ID && id & ID_PSEUDO != 0 {
					debug_assert_eq!(tree.data.root().length, 0, "pseudo object is not zero");
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
		let bg = Background::default();
		bg.run(self.finish_transaction(&bg)).await?;
		bg.drop().await?;
		Ok(self.store)
	}

	/// Get statistics for this sesion.
	pub fn statistics(&self) -> Statistics {
		#[cfg(test)]
		self.verify_cache_usage();

		let data = self.data.borrow();
		Statistics { storage: self.store.statistics(), global_usage: data.lru.size() }
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

fn is_pseudo_id(id: u64) -> bool {
	id != OBJECT_LIST_ID && id & ID_PSEUDO != 0
}
