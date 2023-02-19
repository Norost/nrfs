use {
	super::super::{Cache, IdKey, Tree},
	crate::{resource::Buf, util, Dev, Error, Resource},
	core::{future::Future, pin::Pin},
};

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Evict an entry from the cache.
	pub(super) fn evict_entry(
		&self,
		key: IdKey,
	) -> Option<Pin<Box<dyn Future<Output = Result<(), Error<D>>> + '_>>> {
		trace!("evict_entry {:?}", key);

		// Take entry
		let (data, _, is_dirty) = self.entry_remove(key);

		// If the entry is not dirty, just remove it.
		if !is_dirty {
			trace!(info "not dirty");
			// Unreserve memory
			self.memory_hard_remove_entry(data.len());
			return None;
		}

		self.data.borrow_mut().evict_tasks_count += 1;

		self.busy_insert(key, 0);

		Some(util::box_fut(async move {
			trace!("evict_entry::(background) {:?}", key);

			// Store record.
			let (record_ref, data) = self.store.write(data).await?;

			// Remove from hard limit.
			let data_len = data.len();
			drop(data);
			self.memory_hard_remove_entry(data_len);

			// Store in parent.
			let tree = match key.id {
				super::super::OBJECT_LIST_ID => Tree::object_list(self),
				super::super::OBJECT_BITMAP_ID => Tree::object_bitmap(self),
				id => Tree::object(self, id, key.key.root()),
			};
			tree.update_record(key.key.depth(), key.key.offset(), record_ref)
				.await?;

			let refcount = self.busy_remove(key);

			// If any tasks are waiting on this entry we have to fetch the entry again.
			if refcount > 0 {
				trace!(info "keep");
				// Reserve memory & fetch entry.
				self.busy_insert(key, refcount);
				self.memory_reserve_entry(data_len).await;
				let data = self.store.read(record_ref).await?;
				let refcount = self.busy_remove(key);
				self.entry_insert(key, data, refcount);
			}

			self.entry_unmark_dirty(key);

			let mut data = self.data.borrow_mut();
			data.evict_tasks_count -= 1;
			if data.evict_tasks_count == 0 {
				data.wake_after_evicts.take().map(|w| w.wake());
			}
			drop(data);

			self.verify_cache_usage();

			Ok(())
		}))
	}
}
