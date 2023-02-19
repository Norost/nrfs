use crate::{data::record::Depth, resource::Buf, util, Dev, Error, Resource};

impl<'a, D: Dev, R: Resource> super::Tree<'a, D, R> {
	/// Set a leaf record's data directly.
	///
	/// This avoids a fetch if the entry isn't already present.
	///
	/// `offset` is expressed in record units, not bytes!
	pub async fn set(&self, offset: u64, mut data: R::Buf) -> Result<(), Error<D>> {
		let key = self.id_key(Depth::D0, offset);
		trace!("set {:?}", key);

		util::trim_zeros_end(&mut data);

		if let Some(entry) = self.cache.wait_entry(key).await {
			// If the entry is already present, overwrite it.
			entry.replace(data).await;
		} else {
			// Otherwise insert a new entry.

			self.cache.busy_insert(key, 0);
			self.cache.memory_reserve_entry(data.len()).await;
			let refcount = self.cache.busy_remove(key);

			let mut entry = self.cache.entry_insert(key, data, refcount);
			entry.dirty_records.insert(key.key);
		}

		Ok(())
	}
}
