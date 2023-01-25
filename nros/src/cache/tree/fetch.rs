use {
	super::{Busy, EntryRef, Key, Present, Slot, Tree},
	crate::{resource::Buf, Dev, Error, Record, Resource},
	alloc::rc::Rc,
	core::cell::{RefCell, RefMut},
};

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
	/// Fetch a record for a cache entry.
	pub(super) async fn fetch(
		&self,
		record: &Record,
		busy: &Rc<RefCell<Busy>>,
	) -> Result<EntryRef<'a, D, R>, Error<D>> {
		trace!(
			"fetch {:?} <- ({}, {})",
			busy.borrow_mut().key,
			record.lba,
			record.length
		);

		let entry = self.cache.store.read(record).await?;

		let key = busy.borrow_mut().key;

		let mut comp = self.cache.get_entryref_components(key).expect("no entry");

		let entry = RefMut::map(comp.slot, |slot| {
			let Slot::Busy(busy) = slot else { unreachable!("not busy") };
			busy.borrow_mut().wakers.drain(..).for_each(|w| w.wake());
			let refcount = comp.lru.entry_add(key, busy.clone(), entry.len());

			*slot = Slot::Present(Present { data: entry, refcount });
			let Slot::Present(e) = slot else { unreachable!() };
			e
		});

		Ok(EntryRef::new(
			self.cache,
			key,
			entry,
			comp.dirty_markers,
			comp.lru,
		))
	}

	/// Mark a slot as busy.
	///
	/// # Panics
	///
	/// The slot is not empty.
	pub(super) fn mark_busy(&self, depth: u8, offset: u64) -> Rc<RefCell<Busy>> {
		let data = &mut *self.cache.data.borrow_mut();

		let Some(Slot::Present(obj)) = data.objects.get_mut(&self.id)
			else { unreachable!("no object") };

		let busy = Busy::new(Key::new(0, self.id, depth, offset));

		let level = &mut obj.data.data[usize::from(depth)];
		let prev = level.slots.insert(offset, Slot::Busy(busy.clone()));
		debug_assert!(prev.is_none());

		data.lru.object_increase_refcount(&mut obj.refcount);

		busy
	}
}
