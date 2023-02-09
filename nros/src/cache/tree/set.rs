use {
	super::{Busy, Key, Present, Slot, Tree},
	crate::{resource::Buf, util, Dev, Error, Resource},
};

impl<'a, D: Dev, R: Resource> Tree<'a, D, R> {
	/// Set a leaf record's data directly.
	///
	/// This avoids a fetch if the entry isn't already present.
	///
	/// `offset` is expressed in record units, not bytes!
	pub(super) async fn set(&self, offset: u64, mut data: R::Buf) -> Result<(), Error<D>> {
		trace!(
			"set id {:#x} offset {} data.len {}",
			self.id,
			offset,
			data.len(),
		);

		util::trim_zeros_end(&mut data);

		let key = Key::new(0, self.id, 0, offset);

		if let Some(entry) = self.cache.wait_entry(key).await {
			// If the entry is already present, overwrite it.
			entry.replace(data).await;
		} else {
			// Otherwise insert a new entry.

			// Insert busy entry.
			let (mut obj, mut memory_tracker) = self.cache.fetch_object(self.id).await?;
			let busy = Busy::new(Key::new(0, self.id, 0, offset));
			obj.insert_entry(&mut memory_tracker, 0, offset, Slot::Busy(busy.clone()));
			obj.set_dirty(true);

			// Reserve memory.
			drop((obj, memory_tracker));
			self.cache.memory_reserve_entry(data.len()).await;
			let key = busy.borrow_mut().key;
			busy.borrow_mut().wake_all();

			// Set present.
			let (mut obj, mut memory_tracker) = self.cache.get_object(key.id()).expect("no object");
			let refcount = memory_tracker.finish_fetch_entry(busy, data.len());
			let entry = obj.get_mut(key.depth(), key.offset()).expect("no entry");
			*entry = Slot::Present(Present { data, refcount });

			// Mark dirty
			obj.data.mark_dirty(0, offset, self.max_record_size());
		}

		Ok(())
	}
}
