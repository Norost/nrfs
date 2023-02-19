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
	) -> Result<EntryRef<'a, D, R>, Error<D>> {
		let key = self.id_key(depth, offset);
		trace!("fetch {:?} <- {:?}", key, record_ref);

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

		let data = self.cache.store.read(record_ref).await?;

		self.cache.memory_hard_shrink(estimate - data.len());

		let refcount = self.cache.busy_remove(key);

		let entry = self.cache.entry_insert(key, data, refcount);

		Ok(entry)
	}
}
