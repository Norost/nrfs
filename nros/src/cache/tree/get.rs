use {
	super::{super::Depth, EntryRef, RootLocation, Tree},
	crate::{data::record::RecordRef, util, Dev, Error, Resource},
};

impl<'a, D: Dev, R: Resource> Tree<'a, D, R> {
	/// Get a record.
	pub(in super::super) async fn get(
		&self,
		depth: Depth,
		offset: u64,
	) -> Result<EntryRef<'a, R::Buf>, Error<D>> {
		let key = self.id_key(depth, offset);
		trace!("get {:?}", key);

		// Steps:
		// 1. Try to get the target entry directly.
		// 2. Find first present parent entry in tree.
		// 2a. If not found, fetch root.
		// 3. Work downwards.
		// 4. Presto!

		// 1. Try to get the target entry directly.
		if let Some(entry) = self.cache.wait_entry(key).await {
			return Ok(entry);
		}

		// 2. Find first present parent entry in tree.
		let (mut d, mut offt) = (depth, offset);
		let mut record_ref = loop {
			if d < self.depth() {
				let (p_d, p_offt, p_index) = self.parent_key_index(offt, d);
				let k = self.id_key(p_d, p_offt);
				if let Some(entry) = self.cache.wait_entry(k).await {
					let mut rec_ref = RecordRef::default();
					entry.read(p_index, rec_ref.as_mut());
					break rec_ref;
				}
				(d, offt) = (p_d, p_offt);
				continue;
			}
			// 2a. If not found, fetch root.
			break match &self.root {
				&RootLocation::Object { .. } => {
					let (o_d, o_offt, index) = self.object_key_index();
					let tree = Tree::object_list(self.cache);
					let fut = util::box_fut(tree.get(o_d, o_offt));
					let entry = fut.await?;
					let mut rec_ref = RecordRef::default();
					entry.read(index, rec_ref.as_mut());
					rec_ref
				}
				&RootLocation::ObjectList => self.cache.store.object_list_root(),
				&RootLocation::ObjectBitmap => self.cache.store.object_bitmap_root(),
			};
		};

		// 3. Work downwards.
		loop {
			let entry = self.fetch(d, offt, record_ref).await?;
			if d == depth {
				// 5. Presto!
				return Ok(entry);
			}

			d = d.prev();

			let diff = d as u8 - depth as u8;
			let shift = self.cache.entries_per_parent_p2() * diff;

			offt = offset >> shift;
			let (_, index) = util::divmod_p2(offt, self.cache.entries_per_parent_p2());
			entry.read(index * 8, record_ref.as_mut());
		}
	}
}
