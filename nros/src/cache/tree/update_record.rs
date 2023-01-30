use {
	super::{Busy, Key, Object, Tree, RECORD_SIZE_P2},
	crate::{resource::Buf, util, Dev, Error, Record, Resource},
	alloc::rc::Rc,
	core::{cell::RefCell, mem},
};

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
	/// Update a record.
	/// This will write the record to the parent record or the object of this object.
	///
	/// # Note
	///
	/// `busy` is used to ensure the record is written to the correct location
	/// (object or parent record) in case the tree gets shrunk in the meantime.
	pub(in super::super) async fn update_record(
		self,
		record_depth: u8,
		offset: u64,
		record: Record,
		busy: &Rc<RefCell<Busy>>,
	) -> Result<(), Error<D>> {
		trace!(
			"update_record {:?} ({}, {})",
			Key::new(0, self.id, record_depth, offset),
			record.lba,
			record.length(),
		);

		let Self { mut id, cache, background } = self;

		// The object is guaranteed to exist.
		// At least, if called by a task that holds an entry, which should be guaranteed.
		let cur_object = cache.get_object(id).expect("no object").0.data.object();
		let len = cur_object.total_length();
		let cur_depth = super::depth(cache.max_record_size(), len);
		let parent_depth = record_depth + 1;

		debug_assert!(parent_depth <= cur_depth, "depth out of range");

		let max_offset = super::max_offset(cache.max_record_size(), cur_depth - record_depth);
		debug_assert!(offset < max_offset, "offset out of range");

		let replace_object = |id| {
			debug_assert_eq!(offset, 0, "object can only be at offset 0");

			let (mut obj, _) = cache.get_object(id).expect("no object");
			#[cfg(debug_assertions)]
			obj.data.check_integrity();
			let object = obj.data.object();

			// Ensure the record is actually supposed to be stored at the object.
			let object_depth = super::depth(cache.max_record_size(), object.total_length());
			debug_assert!(parent_depth <= object_depth, "depth out of range");
			if object_depth != parent_depth {
				return false;
			}

			// Copy total length and reference_count to new object.
			let new_object = Object { root: record, ..object };
			trace!(
				info "replace object ({}, {}) -> ({}, {})",
				new_object.root.lba,
				new_object.root.length(),
				object.root.lba,
				object.root.length(),
			);

			// Destroy old root.
			cache.store.destroy(&object.root);

			// Store new object
			// The object is guaranteed to be in the cache as update_record is only called
			// during flush or evict.
			obj.data.set_object(&new_object);

			true
		};

		if cur_depth == parent_depth {
			let replaced = replace_object(id);
			debug_assert!(replaced, "parent_depth is not at object depth");
		} else {
			// Update a parent record.
			// Find parent
			let shift = cache.max_record_size().to_raw() - RECORD_SIZE_P2;
			let (offt, index) = util::divmod_p2(offset, shift);

			let entry = loop {
				let entry = Self::new(cache, background, id)
					.get(parent_depth, offt)
					.await?;

				// If the ID changed but does not match with what the busy entry gave us,
				// check if we should write to the (new!) object instead.
				let entry_key = entry.key;
				let busy_key = busy.borrow_mut().key;
				trace!(info "{:?} >>> {:?} (id: {:#x})", busy_key, entry_key, id);
				if entry_key.id() != busy_key.id() {
					debug_assert!(
						!super::super::is_pseudo_id(id),
						"pseudo objects should not be resized"
					);
					drop(entry);
					if replace_object(busy_key.id()) {
						return Ok(());
					}
				} else {
					break entry;
				}

				// On failure, ensure we have the proper ID, which may have changed due to
				// object_move, and try fetching the entry again.
				id = busy_key.id();
			};

			let old_record = util::get_record(entry.get(), index).unwrap_or_default();
			if old_record.length() == 0 && record.length() == 0 {
				// Both the old and new record are zero, so don't dirty the parent.
				trace!(info "skip both zero");
				debug_assert_eq!(old_record, Record::default());
				debug_assert_eq!(record, Record::default());
				return Ok(());
			}
			entry.modify(background, |data| {
				// Destroy old record
				trace!(
					info "replace parent ({}, {}) -> ({}, {})",
					record.lba,
					record.length(),
					old_record.lba,
					old_record.length(),
				);
				cache.store.destroy(&old_record);

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
