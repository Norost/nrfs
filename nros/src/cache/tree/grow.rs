use {
	super::{Buf, Dev, Error, Key, Object, Present, Record, Resource, Slot, Tree},
	crate::util,
	core::mem,
};

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
	/// Grow record tree.
	pub(super) async fn grow(&self, new_len: u64, &cur_object: &Object) -> Result<(), Error<D>> {
		trace!("grow id {:#x}, new_len {}", self.id, new_len);
		// There are two cases to consider when growing a record tree:
		//
		// * The depth does not change.
		//   Nothing to do then.
		//
		// * The depth changes.
		//   *Move* the object record to a new record and zero out the object record entry.
		//   The dirty new record will bubble up and eventually a new object entry is created.
		//
		// Steps:
		// 1. Adjust levels array size to new_depth.
		// 2. Propagate dirty counters up.
		// 3. Insert parent right above current object record if depth changes.
		//    Do this without await!
		// 4. Update object record.
		//    If depth changes, make it a zero record.
		//    Otherwise, copy cur_object.

		let cur_len = u64::from(cur_object.total_length);

		debug_assert!(
			cur_len < new_len,
			"new len is equal or smaller than cur len"
		);

		let cur_depth = super::depth(self.max_record_size(), cur_len);
		let new_depth = super::depth(self.max_record_size(), new_len);

		// The object is guaranteed to be in the cache since we have cur_object.
		let mut data_ref = self.cache.data.borrow_mut();
		let data = &mut *data_ref;
		let Some(Slot::Present(obj)) = data.objects.get_mut(&self.id)
			else { unreachable!("no object") };

		// 1. Adjust levels array size to new_depth
		let mut v = mem::take(&mut obj.data.data).into_vec();
		debug_assert_eq!(v.len(), usize::from(cur_depth));
		v.resize_with(new_depth.into(), Default::default);
		obj.data.data = v.into();

		// Check if the depth changed.
		// If so we need to move the current object.
		if cur_depth < new_depth && cur_depth > 0 {
			// 2. Propagate dirty counters up.
			// This simply involves inserting counters at 0 offsets.
			// Since we're going to insert a dirty entry, do it unconditionally.
			// Mark first level as dirty.
			let dirty_descendant = cur_depth > 0
				&& obj.data.data[usize::from(cur_depth - 1)]
					.dirty_markers
					.contains_key(&0);
			let [level, levels @ ..] = &mut obj.data.data[usize::from(cur_depth)..]
				else { unreachable!("out of range") };
			let marker = level.dirty_markers.entry(0).or_default();
			marker.is_dirty = true;
			if dirty_descendant {
				marker.children.insert(0);
			}
			// Mark other levels as having a dirty child.
			for lvl in levels.iter_mut() {
				lvl.dirty_markers.entry(0).or_default().children.insert(0);
			}

			// 3. Insert parent right above current object record if depth changes.
			//    Do this without await!
			let key = Key::new(0, self.id, cur_depth, 0);
			let mut d = self.cache.resource().alloc();
			d.resize(core::mem::size_of::<Record>(), 0);
			d.get_mut().copy_from_slice(cur_object.root.as_ref());
			util::trim_zeros_end(&mut d);
			let refcount = data.lru.entry_add_noref(key, d.len());
			let entry = Slot::Present(Present { data: d, refcount });
			obj.add_entry(&mut data.lru, key.depth(), key.offset(), entry);

			// 4. Update object record.
			//    If depth changes, make it a zero record.
			obj.data.set_object(&Object {
				root: Default::default(),
				total_length: new_len.into(),
				..cur_object
			});

			// We just added a record, so evict excess.
			drop(data_ref);
			self.cache.evict_excess(self.background);
		} else {
			// Just adjust length and presto
			obj.data
				.set_object(&Object { total_length: new_len.into(), ..obj.data.object() });
		}

		Ok(())
	}
}
