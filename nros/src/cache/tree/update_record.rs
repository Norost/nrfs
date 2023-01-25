use {
	super::{Busy, Tree, RECORD_SIZE_P2},
	crate::{resource::Buf, util, Dev, Error, Record, Resource},
	alloc::rc::Rc,
	core::{cell::RefCell, mem},
};

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
	/// Update a record.
	/// This will write the record to the parent record or the root of this object.
	///
	/// # Note
	///
	/// `busy` is used to ensure the record is written to the correct location
	/// (root or parent record) in case the tree gets shrunk in the meantime.
	pub(in super::super) async fn update_record(
		&self,
		record_depth: u8,
		offset: u64,
		record: Record,
		busy: &Rc<RefCell<Busy>>,
	) -> Result<(), Error<D>> {
		trace!(
			"update_record {:?} ({}, {})",
			super::Key::new(0, self.id, record_depth, offset),
			record.lba,
			record.length
		);
		// The object is guaranteed to exist.
		// At least, if called by a task that holds an entry, which should be guaranteed.
		let cur_root = self.cache.get_object(self.id).expect("no object").root();
		let len = u64::from(cur_root.total_length);
		let cur_depth = super::depth(self.max_record_size(), len);
		let parent_depth = record_depth + 1;
		debug_assert!(parent_depth <= cur_depth, "depth out of range");

		let max_offset = super::max_offset(self.max_record_size(), cur_depth - record_depth);
		debug_assert!(u128::from(offset) < max_offset, "offset out of range");

		let replace_root = || {
			debug_assert_eq!(offset, 0, "root can only be at offset 0");

			let mut obj = self.cache.get_object(self.id).expect("no object");
			#[cfg(debug_assertions)]
			obj.check_integrity();
			let root = obj.root();

			// Ensure the record is actually supposed to be stored at the root.
			let root_depth = super::depth(self.max_record_size(), root.total_length.into());
			debug_assert!(parent_depth <= root_depth, "depth out of range");
			if root_depth != parent_depth {
				return false;
			}

			// Copy total length and references to new root.
			let new_root =
				Record { total_length: root.total_length, references: root.references, ..record };
			trace!(
				info "replace root ({}, {}) -> ({}, {})",
				new_root.lba,
				new_root.length,
				root.lba,
				root.length
			);

			// Destroy old root.
			self.cache.store.destroy(&root);

			// Store new root
			// The object is guaranteed to be in the cache as update_record is only called
			// during flush or evict.
			obj.set_root(&new_root);

			true
		};

		if cur_depth == parent_depth {
			let replaced = replace_root();
			debug_assert!(replaced, "parent_depth is not at root depth");
		} else {
			// Update a parent record.
			// Find parent
			let shift = self.max_record_size().to_raw() - RECORD_SIZE_P2;
			let (offt, index) = super::divmod_p2(offset, shift);

			let mut entry = self.get(parent_depth, offt).await?;

			// If the ID changed but does not match with what the busy entry gave us,
			// check if we should write to the (new!) root instead.
			let k = entry.key;
			if k.id() != self.id && busy.borrow_mut().key.id() == self.id {
				debug_assert!(
					!super::super::is_pseudo_id(self.id),
					"pseudo objects should not be resized"
				);
				drop(entry);
				if replace_root() {
					return Ok(());
				}
				entry = self.cache.get_entry(k).expect("no entry");
			}

			let old_record = util::get_record(entry.get(), index).unwrap_or_default();
			if old_record.length == 0 && record.length == 0 {
				// Both the old and new record are zero, so don't dirty the parent.
				trace!(info "skip both zero");
				debug_assert_eq!(old_record, Record::default());
				debug_assert_eq!(record, Record::default());
				return Ok(());
			}
			entry.modify(self.background, |data| {
				// Destroy old record
				trace!(
					info "replace parent ({}, {}) -> ({}, {})",
					record.lba,
					record.length,
					old_record.lba,
					old_record.length
				);
				self.cache.store.destroy(&old_record);

				// Calc offset in parent
				let offt = index * mem::size_of::<Record>();
				let min_len = data.len().max(offt + mem::size_of::<Record>());

				// Store new record
				data.resize(min_len, 0);
				data.get_mut()[offt..offt + mem::size_of::<Record>()]
					.copy_from_slice(record.as_ref());
			});
		}
		Ok(())
	}
}
