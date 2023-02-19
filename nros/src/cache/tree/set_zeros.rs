use {
	super::{
		super::{Depth, EntryRef, Key},
		Tree,
	},
	crate::{data::record::RecordRef, resource::Buf, util, Dev, Error, Resource},
};

impl<'a, D: Dev, R: Resource> Tree<'a, D, R> {
	/// Zero out a range of leaf records.
	///
	/// This is more efficient than [`Tree::set`] for clearing large regions.
	pub async fn set_zeros(&self, offset: u64, count: u64) -> Result<(), Error<D>> {
		trace!(
			"set_zeros ({:#x}:{:?}), offset {}, count {}",
			self.id(),
			self.root(),
			offset,
			count,
		);

		if count == 0 {
			return Ok(());
		}

		// Just set empty record and presto.
		if self.depth() == Depth::D0 {
			assert!(offset == 0 && count == 1);
			return self.set(0, self.cache.resource().alloc()).await;
		}

		let shift = |d| self.cache.entries_per_parent_p2() * d as u8;

		// Start from D1 and look for non-zero children.
		let mut depth = Depth::D1;
		let mut offt = offset;
		'z: while offt < offset + count {
			// Find non-empty parent.
			let mut entry = loop {
				let o = offt >> shift(depth);
				let entry = self.get(depth, o).await?;
				let (_, min_index) = util::divmod_p2(o, self.cache.entries_per_parent_p2());
				if entry.len() > min_index * 8 || self.has_dirty(&entry, entry.key.key) {
					break entry;
				}
				if depth >= self.depth() {
					// No non-zero children are left.
					break 'z;
				}
				depth = depth.next();
			};

			// Go down to first D1 parent with non-zero children.
			'y: while depth > Depth::D1 {
				let d = depth.prev();
				let (o, i) = util::divmod_p2(offt >> shift(d), self.cache.entries_per_parent_p2());

				for i in i..1 << self.cache.entries_per_parent_p2() {
					let mut rec = RecordRef::NONE;
					util::read(i * 8, rec.as_mut(), entry.get());

					let co = o << self.cache.entries_per_parent_p2() | u64::try_from(i).unwrap();
					let k = self.id_key(d, co);
					if rec != RecordRef::NONE || self.has_dirty(&entry, k.key) {
						drop(entry);
						entry = if let Some(entry) = self.cache.wait_entry(k).await {
							entry
						} else {
							self.cache.busy_insert(k, 0);
							self.fetch(d, co, rec).await?
						};
						depth = d;
						continue 'y;
					}

					offt += 1 << shift(depth.prev());
					offt &= !((1 << shift(depth.prev())) - 1);
				}
				// In some cases has_dirty returns true for a record we don't need to cover.
				// In this case, continue with the next level.
				trace!(info "no non-zero children");
				continue 'z;
			}

			drop(entry);

			// Just insert a bunch of zero records for now.
			// Not the most efficient but it works.
			let epp = 1 << self.cache.entries_per_parent_p2();
			let end = (offt + epp) & !(epp - 1);
			let end = end.min(offset + count);
			while offt < end {
				self.set(offt, self.cache.resource().alloc()).await?;
				offt += 1;
			}
		}

		Ok(())
	}

	/// Check if the entry *with the given key* is or has any descendants that are dirty.
	fn has_dirty(&self, entry: &EntryRef<'a, D, R>, key: Key) -> bool {
		trace!("has_dirty {:?}", key);
		let mut offt = key.offset();
		let mut end_offt = offt + 1;
		let root = key.root();

		for d in (Depth::D0..=key.depth()).rev() {
			let key = Key::new(root, d, offt);
			let end_key = Key::new(root, d, end_offt);
			if entry.dirty_records.range(key..end_key).next().is_some() {
				return true;
			}
			offt <<= self.cache.entries_per_parent_p2();
			end_offt <<= self.cache.entries_per_parent_p2();
		}

		false
	}
}
