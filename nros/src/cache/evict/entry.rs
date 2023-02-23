use {
	super::super::{mem::BusyState, Cache, Entry, IdKey, Tree},
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
		let (Entry { data, lru_idx }, is_dirty) = self.entry_remove(key);

		// If the entry is not dirty, just remove it.
		if !is_dirty {
			trace!(info "not dirty");
			self.mem()
				.exact_to_empty(self.max_rec_size(), lru_idx, data.len());
			return None;
		}

		self.data().evict_tasks_count += 1;

		let first = self.mem().busy.incr(key);
		assert!(matches!(first, BusyState::New), "not in idle state");

		Some(util::box_fut(async move {
			trace!("evict_entry::(background) {:?}", key);

			let (record_ref, data) = self.store.write(data).await?;

			self.mem()
				.exact_to_empty(self.max_rec_size(), lru_idx, data.len());
			drop(data);

			// Store in parent.
			let tree = match key.id {
				super::super::OBJECT_LIST_ID => Tree::object_list(self),
				super::super::OBJECT_BITMAP_ID => Tree::object_bitmap(self),
				id => Tree::object(self, id, key.key.root()),
			};
			tree.update_record(key.key.depth(), key.key.offset(), record_ref)
				.await?;

			// If any tasks are waiting on this entry we have to fetch the entry again.
			if !self.mem().busy.decr(key) {
				trace!(info "keep");
				// Reserve memory & fetch entry.
				let reserve = self.mem_empty_to_max().await;
				let data = self.store.read(record_ref).await?;
				self.entry_insert(key, data);
				self.mem().busy.mark_ready(key);
			}

			self.entry_unmark_dirty(key);

			let mut data = self.data();
			data.evict_tasks_count -= 1;
			if data.evict_tasks_count == 0 {
				data.wake_after_evicts.take().map(|w| w.wake());
			}
			drop(data);

			Ok(())
		}))
	}
}
