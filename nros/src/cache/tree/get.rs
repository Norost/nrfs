use {
	super::{super::Depth, EntryRef, RootLocation, Tree},
	crate::{data::record::RecordRef, resource::Buf, util, Dev, Error, Resource},
};

impl<'a, D: Dev, R: Resource> Tree<'a, D, R> {
	/// Get a record.
	pub(in super::super) async fn get(
		&self,
		depth: Depth,
		offset: u64,
	) -> Result<EntryRef<'a, D, R>, Error<D>> {
		let key = self.id_key(depth, offset);
		trace!("get {:?}", key);

		// Steps:
		// 1. Check if already present, if so return immediately.
		// 2. Mark as busy.
		// 3. Find first present parent entry in tree.
		// 3a. If not found, fetch root.
		// 4. Work downwards.
		// 5. Presto!

		// 1. Try to get the target entry directly.
		if let Some(entry) = self.cache.wait_entry(key).await {
			return Ok(entry);
		}

		// 2. Mark as busy
		self.cache.busy_insert(key, 0);

		// 3. Find first present parent entry in tree.
		let (mut d, mut offt) = (depth, offset);
		let mut record_ref = loop {
			if d < self.depth() {
				let (p_d, p_offt, p_index) = self.parent_key_index(offt, d);
				let k = self.id_key(p_d, p_offt);
				if let Some(entry) = self.cache.wait_entry(k).await {
					let mut rec_ref = RecordRef::default();
					util::read(p_index, rec_ref.as_mut(), entry.get());
					break rec_ref;
				}

				self.cache.busy_insert(k, 0);
				(d, offt) = (p_d, p_offt);

				continue;
			}
			// 3a. If not found, fetch root.
			break match &self.root {
				&RootLocation::Object { .. } => {
					let (o_d, o_offt, index) = self.object_key_index();
					let tree = Tree::object_list(self.cache);
					let fut = util::box_fut(tree.get(o_d, o_offt));
					let entry = fut.await?;
					let mut rec_ref = RecordRef::default();
					util::read(index, rec_ref.as_mut(), entry.get());
					rec_ref
				}
				&RootLocation::ObjectList => self.cache.store.object_list_root(),
				&RootLocation::ObjectBitmap => self.cache.store.object_bitmap_root(),
			};
		};

		// 4. Work downwards.
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

			util::read(index * 8, record_ref.as_mut(), entry.get());
		}
	}
}
