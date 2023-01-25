use {
	super::{Key, Present, Slot, Tree},
	crate::{resource::Buf, util, Dev, Error, Resource},
};

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
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
			entry.modify(self.background, |d| *d = data);
		} else {
			// Otherwise insert a new entry.
			let (mut obj, mut lru) = self.cache.fetch_object(self.background, self.id).await?;
			let refcount = lru.entry_add_noref(key, data.len());
			let slot = Slot::Present(Present { data, refcount });
			obj.add_entry(&mut lru, 0, offset, slot);
			obj.data.mark_dirty(0, offset, self.max_record_size());
		}

		Ok(())
	}
}
