use crate::{data::record::Depth, util, Dev, Error, Resource};

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

		if let Some(mut entry) = self.cache.wait_entry(key).await {
			entry.replace(data);
		} else {
			self.cache.mem_hard_add().await;
			let mut entry = self.cache.entry_insert(key, data);
			entry.dirty.insert(key);
		}

		Ok(())
	}
}
