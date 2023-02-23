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
			entry.replace(data);
		} else {
			self.cache.mem().busy.incr(key);
			self.cache.mem_empty_to_max().await;
			let mut entry = self.cache.entry_insert(key, data);
			entry.dirty_records.insert(key.key);
		}

		Ok(())
	}
}
