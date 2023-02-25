use {
	super::super::{Cache, IdKey, Tree},
	crate::{util, Dev, Error, Resource},
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
		let (data, is_dirty) = self.entry_remove(key);

		// If the entry is not dirty, just remove it.
		if !is_dirty {
			trace!(info "not dirty");
			self.mem().hard_del();
			return None;
		}

		self.data().evict_tasks_count += 1;

		let first = self.data().busy.incr(key);
		assert!(first, "evicted entry was referenced");

		Some(util::box_fut(async move {
			trace!("evict_entry::(background) {:?}", key);

			let (record_ref, data) = self.store.write(data).await?;
			drop(data);
			self.mem().hard_del();

			// Store in parent.
			let tree = match key.id {
				super::super::OBJECT_LIST_ID => Tree::object_list(self),
				super::super::OBJECT_BITMAP_ID => Tree::object_bitmap(self),
				id => Tree::object(self, id, key.key.root()),
			};
			tree.update_record(key.key.depth(), key.key.offset(), record_ref)
				.await?;

			// If any tasks are waiting on this entry we have to fetch the entry again.
			if !self.data().busy.decr(key) {
				trace!(info "keep");
				// Reserve memory & fetch entry.
				self.data().busy.incr(key);
				self.mem_hard_add().await;
				let data = self.store.read(record_ref).await?;
				self.data().busy.wake(key);
				self.entry_insert(key, data);
			}

			let mut data = self.data();
			data.dirty.remove(&key);

			data.evict_tasks_count -= 1;
			if data.evict_tasks_count == 0 {
				data.wake_after_evicts.take().map(|w| w.wake());
			}
			drop(data);

			Ok(())
		}))
	}
}
