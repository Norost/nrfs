use {
	super::super::{Busy, EntryRef, Key, Present, Slot, SlotExt, Tree},
	crate::{resource::Buf, Dev, Error, Record, Resource},
	alloc::rc::Rc,
	core::cell::{RefCell, RefMut},
};

impl<'a, D: Dev, R: Resource> Tree<'a, D, R> {
	/// Fetch a record for a cache entry.
	pub(super) async fn fetch(
		&self,
		record: &Record,
		busy: Rc<RefCell<Busy>>,
	) -> Result<EntryRef<'a, D, R>, Error<D>> {
		trace!(
			"fetch {:?} <- ({}, {})",
			busy.borrow_mut().key,
			record.lba,
			record.length()
		);

		// We can't make a fully accurate estimate of the size the record will occupy when
		// fetched.
		//
		// There are a couple ways to deal with it:
		// * Always assume maximum size.
		// * Assume maximum size, accounting for compression efficiency.
		// * Use compressed size.
		// Then adjust later.
		//
		// For now, be conservative and assume maximum size.
		let estimate = 1 << self.max_record_size().to_raw();
		self.cache.memory_reserve_entry(estimate).await;

		let entry = self.cache.store.read(record).await?;

		let key = busy.borrow_mut().key;
		let mut comp = self.cache.get_entryref_components(key).expect("no entry");

		let entry = RefMut::map(comp.slot, |slot| {
			debug_assert!(matches!(slot, Slot::Busy(_)), "not busy");

			// Adjust real memory usage properly.
			let refcount = comp.memory_tracker.finish_fetch_entry(busy, estimate);
			comp.memory_tracker.shrink(&refcount, estimate, entry.len());

			*slot = Slot::Present(Present { data: entry, refcount });
			slot.as_present_mut().unwrap()
		});

		Ok(EntryRef::new(
			self.cache,
			key,
			entry,
			comp.dirty_markers,
			comp.memory_tracker,
		))
	}

	/// Mark a slot as busy.
	///
	/// # Panics
	///
	/// The slot is not empty.
	pub(super) fn mark_busy(&self, depth: u8, offset: u64) -> Rc<RefCell<Busy>> {
		let data = &mut *self.cache.data.borrow_mut();
		let obj = data
			.objects
			.get_mut(&self.id)
			.into_present_mut()
			.expect("no object");
		let busy = Busy::new(Key::new(0, self.id, depth, offset));
		obj.insert_entry(
			&mut data.memory_tracker,
			depth,
			offset,
			Slot::Busy(busy.clone()),
		);
		busy
	}
}
