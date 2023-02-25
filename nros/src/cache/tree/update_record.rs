use {
	super::{RootLocation, Tree},
	crate::{
		data::record::{Depth, RecordRef},
		Dev, Error, Resource,
	},
};

impl<'a, D: Dev, R: Resource> Tree<'a, D, R> {
	/// Update a record.
	/// This will write the record to the parent record or the object of this object.
	pub(in super::super) async fn update_record(
		self,
		depth: Depth,
		offset: u64,
		record_ref: RecordRef,
	) -> Result<(), Error<D>> {
		trace!(
			"update_record {:?} {:?}",
			self.id_key(depth, offset),
			record_ref
		);

		assert!(offset < self.max_offset(), "offset out of range");

		let ((p_depth, p_offset, index), p_tree) = if depth == self.depth() {
			// Store in object root
			assert_eq!(offset, 0, "root record can only be at offset 0");
			match self.root {
				RootLocation::Object { .. } => {
					(self.object_key_index(), Tree::object_list(self.cache))
				}
				RootLocation::ObjectList => {
					self.cache.store.set_object_list_root(record_ref);
					return Ok(());
				}
				RootLocation::ObjectBitmap => {
					self.cache.store.set_object_bitmap_root(record_ref);
					return Ok(());
				}
			}
		} else {
			// Store in parent
			(self.parent_key_index(offset, depth), self)
		};

		let mut entry = p_tree.get(p_depth, p_offset).await?;

		let mut old_record_ref = RecordRef::default();
		entry.read(index, old_record_ref.as_mut());

		trace!(
			info "replace {:?} -> {:?} @ ({:#x} {:?} {})",
			record_ref,
			old_record_ref,
			p_tree.id(),
			p_depth,
			p_offset,
		);

		if old_record_ref == RecordRef::NONE && record_ref == RecordRef::NONE {
			// Both the old and new record are zero, so don't dirty the parent.
			trace!(info "skip both zero");
			return Ok(());
		}

		// Destroy old record
		p_tree.cache.store.destroy(old_record_ref);

		// Store new record
		entry.write(index, record_ref.as_ref());

		Ok(())
	}
}
