use {
	super::{slot, Busy, Cache, Key, Lru, Slot},
	crate::{resource::Buf, util::trim_zeros_end, Background, Dev, Resource},
	alloc::rc::Rc,
	core::{
		cell::{RefCell, RefMut},
		future,
		ops::Deref,
		task::Poll,
	},
};

/// Reference to an entry.
pub struct EntryRef<'a, D: Dev, R: Resource> {
	cache: &'a Cache<D, R>,
	pub(super) key: Key,
	lru: RefMut<'a, Lru>,
	entry: RefMut<'a, slot::Present<R::Buf>>,
}

impl<'a, D: Dev, R: Resource> EntryRef<'a, D, R> {
	/// Construct a new [`EntryRef`] for the given entry.
	pub(super) fn new(
		cache: &'a Cache<D, R>,
		key: Key,
		entry: RefMut<'a, slot::Present<R::Buf>>,
		lru: RefMut<'a, Lru>,
	) -> Self {
		Self { cache, key, entry, lru }
	}

	/// Modify the entry's data.
	///
	/// This may trigger a flush when the closure returns.
	///
	/// This consumes the entry to ensure no reference is held across an await point.
	pub fn modify(self, bg: &Background<'a, D>, f: impl FnOnce(&mut R::Buf)) {
		trace!("modify {:?}", self.key);
		let Self { cache, key, mut lru, mut entry } = self;
		let original_len = entry.data.len();

		// Apply modifications.
		f(&mut entry.data);
		// Trim zeros, which we always want to do.
		trim_zeros_end(&mut entry.data);

		lru.entry_adjust(&entry.refcount, original_len, entry.data.len());

		// Update dirty counters if not already dirty.
		drop((lru, entry));
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
	pub(super) fn get_entry(&self, key: Key) -> Option<EntryRef<'_, D, R>> {
		trace!("get_entry {:?}", key);
		let data = self.data.borrow_mut();

		let (trees, lru) = RefMut::map_split(data, |d| (&mut d.objects, &mut d.lru));

		let entry = RefMut::filter_map(trees, |t| {
			let slot = t.get_mut(&key.id())?;
			let Slot::Present(tree) = slot else { return None };
			let level = &mut tree.data.data[usize::from(key.depth())];
			let slot = level.slots.get_mut(&key.offset())?;
			let Slot::Present(entry) = &mut *slot else { return None };
			Some(entry)
		})
		.ok()?;

		Some(EntryRef::new(self, key, entry, lru))
	}

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

			let data = self.data.borrow_mut();
			let (trees, mut lru) = RefMut::map_split(data, |d| (&mut d.objects, &mut d.lru));
			let entry = RefMut::filter_map(trees, |t| {
				let Some(Slot::Present(tree)) = t.get_mut(&key.id()) else {
					debug_assert!(busy.is_none(), "object with reference got removed");
					return None
				};
				let level = &mut tree.data.data[usize::from(key.depth())];
				let entry = level.slots.get_mut(&key.offset())?;
				match entry {
					Slot::Present(entry) => {
						if busy.is_some() {
							lru.entry_decrease_refcount(key, &mut entry.refcount, entry.data.len());
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
				}
			});
			match entry {
				Ok(entry) => Poll::Ready(Some(EntryRef::new(self, key, entry, lru))),
				Err(_) if busy.is_some() => Poll::Pending,
				Err(_) => Poll::Ready(None),
			}
		})
		.await
	}
}
