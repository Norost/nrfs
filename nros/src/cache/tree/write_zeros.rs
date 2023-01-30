use {
	super::{Buf, Dev, Error, Key, Resource, Tree, RECORD_SIZE_P2},
	crate::util,
};

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
	/// Zero out a range of data.
	///
	/// This is more efficient than [`Tree::write`] for clearing large regions.
	pub async fn write_zeros(&self, offset: u64, len: u64) -> Result<u64, Error<D>> {
		trace!(
			"write_zeros id {:#x}, offset {}, len {}",
			self.id,
			offset,
			len,
		);
		// Steps:
		// 1. Trim leftmost record.
		// 2. Clear middle records.
		// 3. Trim rightmost record.

		if len == 0 {
			return Ok(0);
		}

		let object_len = self.len().await?;

		// TODO Cheat a little here so Tree::shrink works properly.
		// We should make an internal write_zeros functions instead though, perhaps?
		let object_depth = super::depth(self.max_record_size(), object_len);
		if object_depth == 0 {
			return Ok(0);
		}
		let object_len = 1u64
			.checked_shl(
				(self.max_record_size().to_raw()
					+ self.cache.entries_per_parent_p2() * (object_depth - 1))
					.into(),
			)
			.unwrap_or(u64::MAX);
		debug_assert_eq!(
			super::depth(self.max_record_size(), object_len),
			object_depth
		);

		// Don't bother if offset exceeds length.
		if offset >= object_len {
			return Ok(0);
		}

		// Restrict offset + len to the end of the object.
		let len = len.min(object_len - offset);

		let end = offset + len - 1;

		let left_offset = offset >> self.max_record_size().to_raw();
		let right_offset = end >> self.max_record_size().to_raw();

		let record_size = 1u64 << self.max_record_size().to_raw();

		let left_trim = usize::try_from(offset % record_size).unwrap();
		let right_trim = usize::try_from(end % record_size).unwrap();

		let mut offset = left_offset;

		let must_trim_left = left_trim != 0 || len < record_size;
		let mut must_trim_right = end % record_size != record_size - 1;

		// u64::MAX is not indexable, so this is a bit of a special case
		must_trim_right &= end != u64::MAX - 1;

		// Trim leftmost record.
		// If the entire record will be cleared, skip and consider it as part of the middle.
		if must_trim_left {
			self.get(0, offset).await?.modify(self.background, |data| {
				if left_offset == right_offset && right_trim < data.len() {
					// We have to trim a single record left & right.
					data.get_mut()[left_trim..=right_trim].fill(0);
					must_trim_right = false;
				} else {
					// We have to trim the leftmost record only on the left.
					data.resize(left_trim, 0);
				}
			});
			offset += 1;
		}

		// Determine whether right record needs to be trimmed or can be cleared entirely.
		if must_trim_right {
			todo!("trim right records");
		//end -= record_size;
		} else {
			// If the object depth is 1, determine if we can just insert a zero record and be done.
			if object_depth == 1 && offset == 0 {
				self.set(0, self.cache.resource().alloc()).await?;
				return Ok(len);
			}
		}

		// Completely zero records not at edges.
		//
		// Since a very large range of the object may need to be zeroed simply inserting leaf
		// records is not an option.
		//
		// To circumvent this, if a zero record is encountered, the loop goes up one level.
		// When a non-zero record is encountered, the loop goes down one level.
		// This allows skipping large amount of leaves quickly.

		let mut depth = 1;
		let shift = |d| d * self.cache.entries_per_parent_p2();

		let entries_per_parent = 1 << self.cache.entries_per_parent_p2();

		let end_offset = end >> self.max_record_size().to_raw();
		let end_object = (object_len - 1) >> self.max_record_size().to_raw();

		'z: while offset <= end_offset {
			// Go up while parent records are zero.
			let mut entry = loop {
				if depth >= object_depth {
					// We're done.
					break 'z;
				}

				let offt = offset >> shift(depth);

				// Check if there is an unvisited non-zero child.
				let entry = self.get(depth, offt).await?;

				let index = (offset >> shift(depth - 1)) % entries_per_parent;

				if entry.len() > usize::try_from(index << RECORD_SIZE_P2).unwrap() {
					// There are non-zero or dirty leafs at or past index.
					break entry;
				}

				// Check if there are any unvisited dirty children.
				if let Some(markers) = entry.dirty_markers.get(&entry.key.offset()) {
					let i = offt * entries_per_parent | index;
					if markers.children.iter().any(|&o| o >= i) {
						break entry;
					}
				}

				// Adjust offset to skip zero records.
				let total_leafs = 1 << shift(depth);
				offset = (offset + total_leafs) & !(total_leafs - 1);
				if offset > end_object {
					// We're *past* the end of the tree.
					break 'z;
				}

				// Go up a level.
				depth += 1;
			};
			debug_assert!(offset <= end_object, "offset out of range");

			// Find the first parent with non-zero or dirty leafs.
			while depth > 1 {
				let dirty_children = &Default::default();
				let dirty_children = entry
					.dirty_markers
					.get(&entry.key.offset())
					.map_or(dirty_children, |m| &m.children);

				// If the entry is empty *and* there are no dirty children,
				// it means all descendants are already zero and we can skip this.
				if entry.len() == 0 && dirty_children.is_empty() {
					// Adjust offset to skip this record in the next iteration.
					let total_leafs = 1 << shift(depth);
					offset = (offset + total_leafs) & !(total_leafs - 1);
					// Go up to parent.
					depth += 1;
					continue 'z;
				}

				let mut index = (offset >> shift(depth - 1)) % entries_per_parent;
				let record = loop {
					debug_assert!(
						index < entries_per_parent,
						"non-zero or dirty child not found"
					);

					// Check for non-zero or dirty
					let record = util::get_record(entry.get(), usize::try_from(index).unwrap())
						.unwrap_or_default();
					let o = entry.key.offset() * entries_per_parent | index;
					if record.length() > 0 || dirty_children.contains(&o) {
						break record;
					}

					index += 1;
				};

				// Fix offset
				// Ensure we don't go below the previous offset.
				offset =
					offset.max((entry.key.offset() << shift(depth)) | (index << shift(depth - 1)));

				// Fetch next entry.
				depth -= 1;
				drop(entry);
				let k = Key::new(0, self.id, depth, offset >> shift(depth));
				entry = if let Some(e) = self.cache.wait_entry(k).await {
					e
				} else {
					let busy = self.mark_busy(k.depth(), k.offset());
					self.fetch(&record, busy).await?
				};
			}
			debug_assert!(offset <= end_object, "offset out of range");

			// Insert zero leafs
			drop(entry);
			let index = offset % entries_per_parent;
			let index_end = (entries_per_parent - 1).min(index + end_offset.saturating_sub(offset));
			for _ in index..index_end + 1 {
				self.set(offset, self.cache.resource().alloc()).await?;
				offset += 1;
			}

			if offset <= end_offset {
				debug_assert_eq!(
					offset % entries_per_parent,
					0,
					"not at end of parent record (offset: {:#x}, end_offset: {:#x})",
					offset,
					end_offset
				);
			}
		}

		Ok(len)
	}
}
