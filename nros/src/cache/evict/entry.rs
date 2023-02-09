use {
	super::super::{Cache, Key, Present, Slot, SlotExt, Tree},
	crate::{resource::Buf, util, Dev, Error, Resource},
	core::{
		future::Future,
		mem,
		pin::{pin, Pin},
	},
	futures_util::FutureExt,
};

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Evict an entry from the cache.
	pub(super) fn evict_entry(
		&self,
		key: Key,
	) -> Option<Pin<Box<dyn Future<Output = Result<(), Error<D>>> + '_>>> {
		trace!("evict_entry {:?}", key);

		// Get object
		let data = &mut *self.data.borrow_mut();
		let obj = data
			.objects
			.get_mut(&key.id())
			.into_present_mut()
			.expect("no object");

		let is_dirty = obj.data.is_marked_dirty(key.depth(), key.offset());

		// Get entry
		let mut entry_entry = obj.occupied(key.depth(), key.offset()).expect("no entry");
		let entry = entry_entry.as_present_mut().expect("not present");

		// Remove from memory tracker (soft limit only!)
		let busy = data
			.memory_tracker
			.soft_remove_entry(&entry.refcount, entry.data.len());

		// If the entry is not dirty, just remove it.
		if !is_dirty {
			trace!(info "not dirty");
			// Unreserve memory
			data.memory_tracker.hard_remove_entry(entry.data.len());
			// Remove slot
			entry_entry.remove();
			// Dereference the corresponding object.
			data.memory_tracker
				.decr_object_refcount(&mut obj.refcount, 1);
			return None;
		}

		// Take entry data
		let entry = mem::replace(&mut entry.data, self.resource().alloc());

		// Insert busy slot
		*entry_entry.get_mut() = Slot::Busy(busy.clone());

		data.evict_tasks_count += 1;

		Some(util::box_fut(async move {
			trace!("evict_entry::(background) {:?}", key);

			// Store record.
			let (record, entry) = self.store.write(entry).await?;

			// Remove from hard limit.
			let entry_len = entry.len();
			drop(entry);
			self.data
				.borrow_mut()
				.memory_tracker
				.hard_remove_entry(entry_len);

			// Store in parent.
			trace!(info "{:?} ~1> {:?}", key, busy.borrow_mut().key);
			let mut key = busy.borrow_mut().key;

			Tree::new(self, key.id())
				.update_record(key.depth(), key.offset(), record, &busy)
				.await?;

			// Unmark as being flushed.
			trace!(info "{:?} ~2> {:?}", key, busy.borrow_mut().key);
			key = busy.borrow_mut().key;

			let mut data = self.data.borrow_mut();
			let data_ref = &mut *data;
			let mut obj = data_ref
				.objects
				.get_mut(&key.id())
				.into_present_mut()
				.expect("no object");

			// If any tasks are waiting on this entry we have to fetch the entry again.
			if busy.borrow_mut().refcount > 0 {
				trace!(info "keep");
				drop(data);

				// Reserve memory & fetch entry.
				self.memory_reserve_entry(entry_len).await;
				let entry = self.store.read(&record).await?;
				key = busy.borrow_mut().key;

				data = self.data.borrow_mut();
				let refcount = data.memory_tracker.finish_fetch_entry(busy, entry_len);

				obj = data
					.objects
					.get_mut(&key.id())
					.into_present_mut()
					.expect("no object");
				let slot = obj.get_mut(key.depth(), key.offset()).expect("no entry");
				*slot = Slot::Present(Present { data: entry, refcount });
			} else {
				// Otherwise just remove
				trace!(info "remove");
				busy.borrow_mut().wake_all();
				obj.remove_entry(&mut data_ref.memory_tracker, key.depth(), key.offset());
			}

			// Remove dirty marker
			let r = obj
				.data
				.unmark_dirty(key.depth(), key.offset(), self.max_record_size());
			debug_assert!(r, "not marked");

			data.evict_tasks_count -= 1;
			if data.evict_tasks_count == 0 {
				data.wake_after_evicts.take().map(|w| w.wake());
			}

			#[cfg(test)]
			{
				drop(data);
				self.verify_cache_usage();
			}

			Ok(())
		}))
	}
}
