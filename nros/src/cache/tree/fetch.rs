use {
	super::super::{Depth, EntryRef, Tree},
	crate::{data::record::RecordRef, resource::Buf, Dev, Error, Resource},
};

impl<'a, D: Dev, R: Resource> Tree<'a, D, R> {
	/// Fetch a record for a cache entry.
	///
	/// The corresponding [`Busy`] entry *must* already be present!
	pub(super) async fn fetch(
		&self,
		depth: Depth,
		offset: u64,
		record_ref: RecordRef,
	) -> Result<EntryRef<'a, R::Buf>, Error<D>> {
		let key = self.id_key(depth, offset);
		trace!("fetch {:?} <- {:?}", key, record_ref);
		let data = self.cache.store.read(record_ref).await?;
		Ok(self.cache.entry_insert(key, data))
	}
}
