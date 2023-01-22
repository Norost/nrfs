use {
	super::{Cache, EntryRef, Key, Tree, RECORD_SIZE_P2},
	crate::{resource::Buf, util::get_record, Background, Dev, Error, Record, Resource},
};

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Get a cache entry of a tree node.
	///
	/// It may fetch up to [`MAX_DEPTH`] of parent entries.
	///
	/// Note that `offset` must already include appropriate shifting.
	///
	/// # Note
	///
	/// This accounts for entries being moved to pseudo-objects while fetching.
	///
	/// The proper ID can be found in the returned entry's `key` field.
	pub(super) async fn tree_fetch_entry<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		key: Key,
	) -> Result<EntryRef<'a, D, R>, Error<D>> {
		trace!("tree_fetch_entry {:?}", key,);

		// Steps:
		//
		// 2. Find the record or the first ancestor that is present.
		//    If found, extract the proper record from it.
		//    If none are present, take the root record.
		// 1. Check if the entry is present.
		//    If so, just return it.
		// 3. Fetch the data associated with the taken record.
		//    Do this "recursively" until the target is reached.

		let rec_size = self.max_record_size().to_raw();

		let mut id = key.id();
		let target_depth = key.depth();
		let offset = key.offset();

		let mut cur_depth = target_depth;
		let depth_offset_shift = |d| (rec_size - RECORD_SIZE_P2) * (d - target_depth);

		let (obj, _) = self.fetch_object(bg, id).await?;
		let root = obj.data.root();
		let len = u64::from(root.total_length);
		drop(obj);

		// Find the first parent or leaf entry that is present starting from a leaf
		// and work back downwards.

		let obj_depth = super::depth(self.max_record_size(), len);

		debug_assert!(
			target_depth < obj_depth,
			"target depth exceeds object depth"
		);

		// FIXME we need to be careful with resizes while this task is running.
		// Perhaps lock object IDs somehow?

		// Go up and check if a parent entry is either present or being fetched.
		let entry = 's: {
			while cur_depth < obj_depth {
				// Check if the entry is already present or being fetched.
				let key = Key::new(0, id, cur_depth, offset >> depth_offset_shift(cur_depth));
				if let Some(entry) = self.wait_entry(key).await {
					id = entry.key.id();
					break 's Some(entry);
				}
				// Try the next level
				cur_depth += 1;
			}
			None
		};

		// Get first record to fetch.
		let mut record;
		// Check if we found any cached record at all.
		if let Some(entry) = entry {
			if cur_depth == target_depth {
				// The entry we need is already present
				return Ok(entry);
			}

			// Start from a parent record.
			debug_assert!(cur_depth < obj_depth, "parent should be below root");
			cur_depth -= 1;
			let offt = offset >> depth_offset_shift(cur_depth);
			let index = (offt % (1 << rec_size - RECORD_SIZE_P2))
				.try_into()
				.unwrap();
			record = get_record(entry.get(), index).unwrap_or_default();
		} else {
			// Start from the root.
			debug_assert_eq!(cur_depth, obj_depth, "root should be at obj_depth");
			record = root;
			cur_depth -= 1;
		}

		// Fetch records until we can lock the one we need.
		debug_assert!(cur_depth >= target_depth);
		let entry = loop {
			if record.length == 0 {
				// Skip straight to the end since it's all zeroes from here on anyways.
				let key = Key::new(0, id, target_depth, offset);
				return self.fetch_entry(bg, key, &Record::default()).await;
			}

			let key = Key::new(0, id, cur_depth, offset >> depth_offset_shift(cur_depth));
			let entry = self.fetch_entry(bg, key, &record).await?;
			id = entry.key.id();

			// Check if we got the record we need.
			if cur_depth == target_depth {
				break entry;
			}

			cur_depth -= 1;

			// Fetch the next record.
			let offt = offset >> depth_offset_shift(cur_depth);
			let index = (offt % (1 << rec_size - RECORD_SIZE_P2))
				.try_into()
				.unwrap();
			record = get_record(entry.get(), index).unwrap_or_default();
		};

		Ok(entry)
	}
}

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
	/// Get a cache entry of a tree node.
	///
	/// It may fetch up to [`MAX_DEPTH`] of parent entries.
	///
	/// Note that `offset` must already include appropriate shifting.
	pub(super) async fn get(&self, depth: u8, offset: u64) -> Result<EntryRef<'a, D, R>, Error<D>> {
		let key = Key::new(0, self.id, depth, offset);
		self.cache.tree_fetch_entry(self.background, key).await
	}
}
