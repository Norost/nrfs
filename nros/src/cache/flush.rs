use {
	super::{Cache, Entry, EntryRef, IdKey, Tree, OBJECT_BITMAP_ID, OBJECT_LIST_ID},
	crate::{data::record::Depth, resource::Buf, waker_queue, Dev, Error, Resource},
	core::{future, task::Poll},
	futures_util::stream::{FuturesUnordered, TryStreamExt},
};

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Flush an entry from the cache.
	///
	/// This does not evict the entry.
	///
	/// Does nothing if the entry wasn't present or dirty.
	async fn flush_entry(&self, key: IdKey) -> Result<(), Error<D>> {
		trace!("flush_entry {:?}", key);

		// Wait for entry
		// Don't use wait_entry as that may redundantly fetch the entry again.
		let Some(entry) = self.wait_entry_nofetch(key).await else { return Ok(()) };

		// Check if dirty.
		// Remove mark while at it.
		if !entry.dirty_records.contains(&key.key) {
			trace!(info "not dirty");
			// The entry is not dirty, so skip.
			return Ok(());
		}

		// Take entry.
		drop(entry);
		let (Entry { data, lru_idx }, _) = self.entry_remove(key);

		// Store entry.
		let (rec, data) = self.store.write(data).await?;

		// TODO check if tree can allocate enough reserved memory to operate
		// If not, discard entry to avoid potential deadlock.
		// TODO check LRU too.
		// FIXME don't just fucking discard goddamn

		let data_len = data.len();
		let data = if false {
			Some(data)
		} else {
			self.mem()
				.exact_to_empty(self.max_rec_size(), lru_idx, data.len());
			drop(data);
			None
		};

		let tree = match key.id {
			super::OBJECT_LIST_ID => Tree::object_list(self),
			super::OBJECT_BITMAP_ID => Tree::object_bitmap(self),
			id => Tree::object(self, id, key.key.root()),
		};
		tree.update_record(key.key.depth(), key.key.offset(), rec)
			.await?;

		// Fetch entry again if a task needs it.
		let refd = self.mem().busy.decr(key);
		if let Some(data) = data {
			self.entry_insert(key, data);
		} else if refd {
			self.mem_empty_to_max().await;
			let data = self.store.read(rec).await?;
			self.entry_insert(key, data);
		}

		self.entry_unmark_dirty(key);

		Ok(())
	}

	/// Flush all entries.
	pub(super) async fn flush_all(&self) -> Result<(), Error<D>> {
		trace!("flush_all");
		// Go through all trees and flush from bottom to top.
		//
		// Start from the bottom of all trees since those are trivial to all flush in parallel.

		// Helper function for entirely flushing an object, bottom to top.
		let flush_object = |id| async move {
			trace!("flush_all::flush_object {:#x}", id);
			// Bottom to top
			for d in Depth::D0..=Depth::D3 {
				let data = self.data();
				let Some(obj) = data.objects.get(&id) else { return Ok(()) };
				// Collect all offsets
				let offt = obj
					.dirty_records
					.iter()
					.copied()
					.filter(|k| k.depth() == d)
					.collect::<Vec<_>>();
				// Flush all entries at current level.
				drop(data);
				offt.into_iter()
					.map(|key| self.flush_entry(IdKey { id, key }))
					.collect::<FuturesUnordered<_>>()
					.try_for_each(|()| async { Ok(()) })
					.await?;
			}
			Ok(())
		};
		#[cfg(feature = "trace")]
		let flush_object = |id| crate::trace::TracedTask::new(flush_object(id));

		let data = || self.data();

		self.wait_all_evict().await;

		// Flush all objects except the object list & bitmap,
		// since the latter will get a lot of updates to the leaves.
		let queue = data()
			.objects
			.keys()
			.copied()
			.filter(|&id| id & (1 << 58) == 0)
			.map(flush_object)
			.collect::<FuturesUnordered<_>>();
		queue.try_for_each(|()| async { Ok(()) }).await?;

		// Wait for evicts to finish.
		self.wait_all_evict().await;

		// Now flush the object list and bitmap.
		[OBJECT_LIST_ID, OBJECT_BITMAP_ID]
			.into_iter()
			.map(flush_object)
			.collect::<FuturesUnordered<_>>()
			.try_for_each(|()| async { Ok(()) })
			.await?;

		// Wait for evicts to finish.
		self.wait_all_evict().await;

		// Tadha!
		// Do a sanity check just in case.
		if cfg!(debug_assertions) {
			let data = self.data();
			for (&_id, obj) in data.objects.iter() {
				assert!(obj.dirty_records.is_empty(), "flush_all didn't flush all");
			}
		}
		Ok(())
	}

	async fn wait_all_evict(&self) {
		trace!("wait_all_evict");
		future::poll_fn(|cx| {
			let mut data = self.data();
			if data.evict_tasks_count != 0 {
				data.wake_after_evicts = Some(cx.waker().clone());
				Poll::Pending
			} else {
				trace!(info "all entries evicted");
				Poll::Ready(())
			}
		})
		.await
	}
}
