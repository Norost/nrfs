use {
	super::{tree::data::Dirty, Busy, Cache, Key, Lru, Present, Slot},
	crate::{resource::Buf, util::trim_zeros_end, Background, Dev, Resource},
	alloc::rc::Rc,
	core::{
		cell::{RefCell, RefMut},
		future,
		ops::Deref,
		task::Poll,
	},
	rustc_hash::FxHashMap,
};

/// Reference to an entry.
pub struct EntryRef<'a, D: Dev, R: Resource> {
	cache: &'a Cache<D, R>,
	pub(super) key: Key,
	lru: RefMut<'a, Lru>,
	entry: RefMut<'a, Present<R::Buf>>,
	pub(super) dirty_markers: RefMut<'a, FxHashMap<u64, Dirty>>,
}

impl<'a, D: Dev, R: Resource> EntryRef<'a, D, R> {
	/// Construct a new [`EntryRef`] for the given entry.
	///
	/// This puts the entry at the back of the LRU queue.
	pub(super) fn new(
		cache: &'a Cache<D, R>,
		key: Key,
		entry: RefMut<'a, Present<R::Buf>>,
		dirty_markers: RefMut<'a, FxHashMap<u64, Dirty>>,
		mut lru: RefMut<'a, Lru>,
	) -> Self {
		lru.touch(&entry.refcount);
		Self { cache, key, entry, dirty_markers, lru }
	}

	/// Modify the entry's data.
	///
	/// This may trigger a flush when the closure returns.
	///
	/// This consumes the entry to ensure no reference is held across an await point.
	pub fn modify(self, bg: &Background<'a, D>, f: impl FnOnce(&mut R::Buf)) {
		trace!("modify {:?}", self.key);
		let Self { cache, key, mut lru, mut entry, dirty_markers } = self;
		let original_len = entry.data.len();

		// Apply modifications.
		f(&mut entry.data);
		// Trim zeros, which we always want to do.
		trim_zeros_end(&mut entry.data);

		lru.entry_adjust(&entry.refcount, original_len, entry.data.len());

		// Update dirty counters if not already dirty.
		drop((lru, entry, dirty_markers));
		let mut obj = self.cache.get_object(key.id()).expect("no object");
		obj.mark_dirty(key.depth(), key.offset(), cache.max_record_size());
		drop(obj);

		// Flush
		cache.evict_excess(bg);
	}
}

impl<'a, D: Dev, R: Resource> Deref for EntryRef<'a, D, R> {
	type Target = R::Buf;

	fn deref(&self) -> &Self::Target {
		&self.entry.data
	}
}

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Try to get an entry directly.
	///
	/// This will block if a task is busy with the entry.
	pub(super) async fn wait_entry(&self, mut key: Key) -> Option<EntryRef<'_, D, R>> {
		trace!("wait_entry {:?}", key);
		let mut busy = None::<Rc<RefCell<Busy>>>;
		future::poll_fn(|cx| {
			if let Some(busy) = busy.as_mut() {
				key = busy.borrow_mut().key;
			}

			let Some(mut comp) = self.get_entryref_components(key) else {
				debug_assert!(busy.is_none(), "entry removed while waiting");
				return Poll::Ready(None)
			};

			let entry = RefMut::filter_map(comp.slot, |slot| match slot {
				Slot::Present(entry) => {
					if busy.is_some() {
						comp.lru.entry_decrease_refcount(
							key,
							&mut entry.refcount,
							entry.data.len(),
						);
					}
					Some(entry)
				}
				Slot::Busy(entry) => {
					let mut e = entry.borrow_mut();
					e.wakers.push(cx.waker().clone());
					if busy.is_none() {
						e.refcount += 1;
						busy = Some(entry.clone());
					}
					None
				}
			});
			match entry {
				Ok(entry) => Poll::Ready(Some(EntryRef::new(
					self,
					key,
					entry,
					comp.dirty_markers,
					comp.lru,
				))),
				Err(_) if busy.is_some() => Poll::Pending,
				Err(_) => Poll::Ready(None),
			}
		})
		.await
	}

	/// Get [`RefMut`]s to components necessary to construct a [`EntryRef`].
	pub(super) fn get_entryref_components(&self, key: Key) -> Option<EntryRefComponents<'_, R>> {
		let data = self.data.borrow_mut();

		let (objects, lru) = RefMut::map_split(data, |d| (&mut d.objects, &mut d.lru));

		let level = RefMut::filter_map(objects, |objects| {
			let slot = objects.get_mut(&key.id())?;
			let Slot::Present(obj) = slot else { return None };
			Some(&mut obj.data.data[usize::from(key.depth())])
		})
		.ok()?;

		let (slots, dirty_markers) =
			RefMut::map_split(level, |level| (&mut level.slots, &mut level.dirty_markers));

		let slot = RefMut::filter_map(slots, |slots| slots.get_mut(&key.offset())).ok()?;

		Some(EntryRefComponents { lru, slot, dirty_markers })
	}

	/// Try to get an entry directly.
	pub(super) fn get_entry(&self, key: Key) -> Option<EntryRef<'_, D, R>> {
		let EntryRefComponents { lru, slot, dirty_markers } = self.get_entryref_components(key)?;
		let entry = RefMut::filter_map(slot, |s| {
			let Slot::Present(e) = s else { return None };
			Some(e)
		})
		.ok()?;
		Some(EntryRef::new(self, key, entry, dirty_markers, lru))
	}
}

pub(super) struct EntryRefComponents<'a, R: Resource> {
	pub lru: RefMut<'a, Lru>,
	pub slot: RefMut<'a, Slot<R::Buf>>,
	pub dirty_markers: RefMut<'a, FxHashMap<u64, Dirty>>,
}
