use {
	super::{Busy, EntryRef, Key, Slot, Tree},
	crate::{resource::Buf, util, Dev, Error, Resource},
	alloc::rc::Rc,
	core::cell::RefCell,
};

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
	/// Get a cache entry of a tree node.
	///
	/// It may fetch up to [`MAX_DEPTH`] of parent busy_entries.
	///
	/// Note that `offset` must already include appropriate shifting.
	///
	/// # Note
	///
	/// This accounts for busy_entries being moved to pseudo-objects while fetching.
	///
	/// The proper ID can be found in the returned entry's `key` field.
	pub(super) async fn get(&self, depth: u8, offset: u64) -> Result<EntryRef<'a, D, R>, Error<D>> {
		let key = Key::new(0, self.id, depth, offset);

		trace!("get {:?}", key);

		// This function is tricky since it has to account for any amount of grows and shrinks
		// while it is fetching an entry.
		//
		// To deal with it, it checks for chain breaks,
		// e.g. if an object (1) got moved to (2), shrunk (3), then grown the state of
		// the task may be
		//
		//      |1|         |2|         |3|         |3| (|2|)      d2
		//      /           /           /           /
		//    |1|   ==>   |2|   ==>   |2|   ==>   |2|              d1
		//    /           /           /           /
		//  |1|         |2|         |2|         |2|                d0
		//
		// Note that in the last step the chain is still working from |3|d2, so it needs to go
		// up to fetch |2|d2 which is the real parent of the target entry.

		// Steps:
		// 1. Try to get the target entry directly.
		// 2. On failure, insert a busy slot.
		// 3. Find record of current entry to fetch.
		// 3a. Take root of object if at top.
		// 3b. Find first parent already in cache.
		// 3bI. If key.id() of the child & parent differ (chain break),
		//      get root of object of child,
		//      repeat from 3.
		// 3c. Insert a busy slot on failure, repeat from 3.
		// 4. Go down to target entry.
		// 4a. If target entry, return.
		// 4b. If key.id() of the child & parent differ (chain break),
		//     get root of object of child,
		//     repeat from 3.
		// 4c. Get record of child to fetch next.

		// 1. Try to get the target entry directly.
		if let Some(entry) = self.cache.wait_entry(key).await {
			return Ok(entry);
		}

		// 2. On failure, insert a busy slot.
		let (mut obj, mut lru) = self.cache.fetch_object(self.background, self.id).await?;
		let busy = Busy::new(key);
		obj.add_entry(&mut lru, depth, offset, Slot::Busy(busy.clone()));

		let mut busy_entries = vec![busy];
		let mut cur_depth = depth;
		let mut root = obj.data.root();
		let mut id = key.id();

		// Helper functions.
		let shift = |d| (d - depth) * self.cache.entries_per_parent_p2();
		let get_record = |entry: &[u8], cur_depth| {
			let offt = offset >> shift(cur_depth);
			let index = offt as usize % (1 << self.cache.entries_per_parent_p2());
			util::get_record(entry, index).unwrap_or_default()
		};
		let check_chain_break = |busy_entries: &[Rc<RefCell<Busy>>], entry: EntryRef<'_, _, _>| {
			let child_busy = busy_entries.last().unwrap();
			let id = child_busy.borrow_mut().key.id();
			// If the ID of the (supposed) parent and child differ,
			// try fetching the child again.
			// The root may have changed, so fetch it again too.
			(entry.key.id() != id).then(|| {
				trace!(info "chain break {:#x} >>> {:#x}", entry.key.id(), id);
				drop(entry);
				let (obj, _) = self.cache.get_object(id).expect("no object");
				(id, obj.data.root())
			})
		};

		drop((obj, lru));

		loop {
			// 3. Find record of current entry to fetch.
			let mut record = loop {
				let obj_depth = super::depth(self.max_record_size(), root.total_length.into());
				// 3a. Take root of object if at top.
				debug_assert!(cur_depth < obj_depth);
				if obj_depth == cur_depth + 1 {
					trace!(info "fetch root");
					break root;
				}

				// 3b. Find first parent already in cache.
				let k = Key::new(0, id, cur_depth + 1, offset >> shift(cur_depth + 1));
				if let Some(entry) = self.cache.wait_entry(k).await {
					trace!(info "fetch from parent {:?}", k);
					let record = get_record(entry.get(), cur_depth);
					// 3bI. If key.id() of the child & parent differ (chain break),
					//      get root of object of child,
					//      repeat from 3.
					if let Some(new_param) = check_chain_break(&busy_entries, entry) {
						(id, root) = new_param;
						continue;
					}
					break record;
				}

				// 3c. Insert a busy slot on failure.
				let (mut obj, mut lru) = self.cache.get_object(k.id()).expect("no object");
				let busy = Busy::new(k);
				obj.add_entry(&mut lru, k.depth(), k.offset(), Slot::Busy(busy.clone()));
				busy_entries.push(busy);

				cur_depth += 1;
			};

			// 4. Go down to target entry.
			loop {
				let busy = busy_entries.pop().expect("no busy entries");
				let entry = self.fetch(&record, busy).await?;

				// 4a. If target entry, return.
				if busy_entries.is_empty() {
					return Ok(entry);
				}

				// 4c. Get record of child to fetch next. (1)
				record = get_record(entry.get(), cur_depth - 1);

				// 4b. If key.id() of the child & parent differ,
				//     get root of object of child,
				//     repeat from 3.
				if let Some(new_param) = check_chain_break(&busy_entries, entry) {
					(id, root) = new_param;
					break;
				}

				// 4c. Get record of child to fetch next. (2)
				cur_depth -= 1;
			}
		}
	}
}
