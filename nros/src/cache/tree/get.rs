use {
	super::{EntryRef, Key, Tree, RECORD_SIZE_P2},
	crate::{resource::Buf, util, Dev, Error, Resource},
};

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
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
	pub(super) async fn get(&self, depth: u8, offset: u64) -> Result<EntryRef<'a, D, R>, Error<D>> {
		trace!("get {:?}", Key::new(0, self.id, depth, offset));

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

		let target_depth = depth;

		let mut cur_depth = target_depth;
		let depth_offset_shift = |d| (rec_size - RECORD_SIZE_P2) * (d - target_depth);

		let len = self.len().await?;

		// Find the first parent or leaf entry that is present starting from a leaf
		// and work back downwards.

		let obj_depth = super::depth(self.max_record_size(), len);

		debug_assert!(
			target_depth < obj_depth,
			"target depth exceeds object depth"
		);

		let mut busy_entries = vec![];

		// Go up and check if a parent entry is either present or being fetched.
		let entry = 's: {
			while cur_depth < obj_depth {
				// Check if the entry is already present or being fetched.
				let k = Key::new(
					0,
					self.id,
					cur_depth,
					offset >> depth_offset_shift(cur_depth),
				);
				if let Some(entry) = self.cache.wait_entry(k).await {
					break 's Some(entry);
				}

				// Insert busy task to prevent races.
				let busy = self.mark_busy(k.depth(), k.offset());
				busy_entries.push(busy);

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
				debug_assert!(busy_entries.is_empty());
				// The entry we need is already present
				return Ok(entry);
			}

			// Start from a parent record.
			debug_assert!(cur_depth < obj_depth, "parent should be below root");
			let offt = offset >> depth_offset_shift(cur_depth - 1);
			let index = (offt % (1 << rec_size - RECORD_SIZE_P2))
				.try_into()
				.unwrap();
			record = util::get_record(entry.get(), index).unwrap_or_default();
		} else {
			// Start from the root.
			debug_assert_eq!(cur_depth, obj_depth, "root should be at obj_depth");
			// We will fetch it below at the "hop", so just set to default value.
			record = Default::default();
		}

		// Fetch records until we can lock the one we need.
		debug_assert!(cur_depth >= target_depth);
		let entry = loop {
			trace!("get::read ({}, {})", record.lba, record.length);

			// Get the key, which may also point to a new ID in case of shrink
			let k = busy_entries.last().unwrap().borrow_mut().key;

			// Check if we need to "hop" to a new object.
			//
			// e.g. after shrink the chain may look like this:
			//
			//     |1|            |2|
			//     /              /
			//   |1|      =>    |1|
			//   /              /
			// |1|            |1|
			//
			// Between |2| and |1| a hop is necessary,
			// i.e. we need to get the new/real root of |1|.
			let obj = self.cache.get_object(k.id()).unwrap();
			#[cfg(debug_assertions)]
			obj.check_integrity();
			if super::depth(self.max_record_size(), obj.root().total_length.into()) == k.depth() + 1
			{
				record = obj.root();
			}
			drop(obj);

			// Read record
			let busy = busy_entries
				.last()
				.expect("out of busy slots before target depth");
			let entry = self.fetch(&record, busy).await?;
			busy_entries.pop();

			// Check if we got the record we need.
			if busy_entries.is_empty() {
				break entry;
			}

			// Fetch the next record.
			let offt = offset >> depth_offset_shift(k.depth() - 1);
			let index = (offt % (1 << rec_size - RECORD_SIZE_P2))
				.try_into()
				.unwrap();
			record = util::get_record(entry.get(), index).unwrap_or_default();
		};

		Ok(entry)
	}
}
