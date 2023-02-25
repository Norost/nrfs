use {
	super::super::{Depth, EntryRef, Tree},
	crate::{data::record::RecordRef, Dev, Error, Resource},
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
		self.cache.mem_hard_add().await;
		let data = self.cache.store.read(record_ref).await?;
		self.cache.data().busy.wake(key);
		Ok(self.cache.entry_insert(key, data))
	}
}
