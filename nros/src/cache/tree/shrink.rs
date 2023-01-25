use {
	super::{
		data::TreeData, Buf, Busy, Dev, Error, Key, Present, Record, RefCount, Resource, Slot, Tree,
	},
	crate::util,
	core::mem,
};

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
	/// Shrink record tree.
	pub(super) async fn shrink(&self, new_len: u64, &cur_root: &Record) -> Result<(), Error<D>> {
		trace!("shrink id {:#x}, new_len {}", self.id, new_len);
		// Steps:
		// 1. If the depth changes, find new root.
		// 1a. Split object into two: current with new root and pseudo-object with records to
		//     be zeroed.
		// 2. Zero out data past new_len.

		let cur_len = u64::from(cur_root.total_length);

		debug_assert!(
			new_len < cur_len,
			"new len is equal or greater than cur len"
		);

		let cur_depth = super::depth(self.max_record_size(), cur_len);
		let new_depth = super::depth(self.max_record_size(), new_len);
		{
			trace!("depth: {} -> {}", cur_depth, new_depth);
		}

		if new_depth < cur_depth {
			// 1. If the depth changes, find new root.
			let new_root = if new_depth == 0 {
				Record::default()
			} else {
				// Take new root out
				let parent = self.get(new_depth, 0).await?;
				let rec = util::get_record(parent.get(), 0).unwrap_or_default();
				parent.modify(self.background, |d| {
					if d.len() > mem::size_of::<Record>() {
						d.get_mut()[..mem::size_of::<Record>()].fill(0)
					} else {
						d.resize(0, 0)
					}
				});
				rec
			};
			let new_root = Record {
				references: cur_root.references,
				total_length: new_len.into(),
				_reserved: 1,
				..new_root
			};

			// 1a. Split object into two: current with new root and pseudo-object with records to
			//     be zeroed.

			// Create a pseudo object.
			// This will be swapped with the current object.
			let mut data_ref = self.cache.data.borrow_mut();
			let data = &mut *data_ref;
			let pseudo_id = data.new_pseudo_id();
			let Some(Slot::Present(cur_obj)) = data.objects.get_mut(&self.id)
				else { unreachable!("no object") };
			let mut pseudo_obj = TreeData::new(new_root, self.max_record_size());

			#[cfg(debug_assertions)]
			cur_obj.data.check_integrity();

			// Transfer all entries with offset *inside* the range of the "new" object to pseudo
			// object
			let mut offt = super::max_offset(self.max_record_size(), new_depth);
			for d in 0..new_depth {
				let cur_level = &mut cur_obj.data.data[usize::from(d)];
				let pseudo_level = &mut pseudo_obj.data[usize::from(d)];

				// Move entries
				for (offset, slot) in cur_level.slots.drain_filter(|k, _| u128::from(*k) < offt) {
					pseudo_level.slots.insert(offset, slot);
				}

				// Move markers
				for (offset, marker) in cur_level
					.dirty_markers
					.drain_filter(|k, _| u128::from(*k) < offt)
				{
					pseudo_level.dirty_markers.insert(offset, marker);
				}

				offt >>= self.cache.entries_per_parent_p2();
			}
			// Fix markers for cur_obj
			if new_depth > 0 {
				let marker = cur_obj.data.data[usize::from(new_depth)]
					.dirty_markers
					.get_mut(&0);
				if let Some(marker) = marker {
					marker.children.remove(&0);
					debug_assert!(marker.is_dirty, "modified parent not dirty");
				}
			}

			mem::swap(&mut cur_obj.data, &mut pseudo_obj);

			// If the pseudo-object has no entries, it's already zeroed and there is nothing
			// left to do.
			let refcount = pseudo_obj.data.iter().fold(0, |x, lvl| x + lvl.slots.len());
			if refcount > 0 {
				// Adjust refcount of cur_obj.
				if data
					.lru
					.object_decrease_refcount(self.id, &mut cur_obj.refcount, refcount)
				{
					unreachable!("pseudo object got shrunk?");
				}

				// Fix keys of LRU entries & any busy tasks.
				for lvl in pseudo_obj.data.iter_mut() {
					for slot in lvl.slots.values() {
						let f =
							|key: Key| Key::new(key.flags(), pseudo_id, key.depth(), key.offset());
						match slot {
							Slot::Present(Present {
								refcount: RefCount::NoRef { lru_index },
								..
							}) => {
								let key = data.lru.get_mut(*lru_index).expect("no lru entry");
								*key = f(*key);
							}
							Slot::Present(Present { refcount: RefCount::Ref { busy }, .. })
							| Slot::Busy(busy) => {
								let mut busy = busy.borrow_mut();
								busy.key = f(busy.key);
							}
						}
					}
				}
			}

			let busy = Busy::with_refcount(Key::new(Key::FLAG_OBJECT, pseudo_id, 0, 0), refcount);
			let refcount = RefCount::Ref { busy };

			// Insert pseudo-object.
			let present = Present { data: pseudo_obj, refcount };
			data.objects.insert(pseudo_id, Slot::Present(present));

			// Zero out pseudo-object.
			drop(data_ref);
			Tree::new(self.cache, self.background, pseudo_id)
				.write_zeros(0, u64::MAX)
				.await?;
		} else {
			// Just change the length.
			let mut data = self.cache.data.borrow_mut();
			let Some(Slot::Present(cur_obj)) = data.objects.get_mut(&self.id)
				else { unreachable!("no object") };
			cur_obj
				.data
				.set_root(&Record { total_length: new_len.into(), ..cur_obj.data.root() });
		}

		// Zero out data written past the end.
		self.write_zeros(new_len, u64::MAX).await?;

		Ok(())
	}
}
