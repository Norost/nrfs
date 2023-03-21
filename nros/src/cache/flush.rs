use {
	super::{Cache, IdKey, Key, RootIndex, Tree, OBJECT_BITMAP_ID, OBJECT_LIST_ID},
	crate::{data::record::Depth, Dev, Error, Resource},
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
		let Some(entry) = self.wait_entry(key).await else { return Ok(()) };

		// Check if dirty.
		if !entry.dirty.contains(&key) {
			trace!(info "not dirty");
			// The entry is not dirty, so skip.
			return Ok(());
		}

		// Take entry.
		drop(entry);
		self.data().busy.incr(key);
		let (data, _) = self.entry_remove(key);

		// Store entry.
		let (rec, data) = self.store.write(data).await?;

		// TODO check if tree can allocate enough reserved memory to operate
		// If not, discard entry to avoid potential deadlock.
		// TODO check LRU too.
		// FIXME don't just fucking discard goddamn

		let data = if false {
			Some(data)
		} else {
			drop(data);
			self.mem().hard_del();
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
		if let Some(data) = data {
			self.entry_insert(key, data);
		} else if self.data().busy.decr(key) {
			self.data().busy.incr(key);
			self.mem_hard_add().await;
			let data = self.store.read(rec).await?;
			self.entry_insert(key, data);
		}

		self.data().dirty.remove(&key);

		Ok(())
	}

	/// Flush all entries.
	pub(super) async fn flush_all(&self) -> Result<(), Error<D>> {
		trace!("flush_all");

		// TODO optimize this crap.

		// Go through all trees and flush from bottom to top.
		//
		// Start from the bottom of all trees since those are trivial to all flush in parallel.

		// Helper function for entirely flushing an object, bottom to top.
		let flush_object = |id, root: super::RootIndex| async move {
			trace!("flush_all::flush_object ({:#x}:{:?})", id, root);
			for d in Depth::D0..=Depth::D3 {
				let start = IdKey { id, key: Key::new(root, d, 0) };
				let end = IdKey { id, key: Key::new(root, d, Key::MAX_OFFSET) };
				let keys = self
					.data()
					.dirty
					.range(start..=end)
					.copied()
					.collect::<Vec<_>>();
				// Flush all entries at current level.
				keys.into_iter()
					.map(|key| self.flush_entry(key))
					.collect::<FuturesUnordered<_>>()
					.try_for_each(|()| async { Ok(()) })
					.await?;
			}
			Ok(())
		};
		#[cfg(feature = "trace")]
		let flush_object = |id, root| crate::trace::TracedTask::new(flush_object(id, root));

		self.wait_all_evict().await;

		// Flush all objects except the object list & bitmap,
		// since the latter will get a lot of updates to the leaves.
		let mut prev_id = None;
		let queue = self
			.data()
			.dirty
			.iter()
			.flat_map(|key| (Some(key.id) != prev_id).then(|| *prev_id.insert(key.id)))
			.filter(|id| ![OBJECT_LIST_ID, OBJECT_BITMAP_ID].contains(id))
			.flat_map(|id| (RootIndex::I0..=RootIndex::I3).map(move |r| (id, r)))
			.map(|(id, root)| flush_object(id, root))
			.collect::<FuturesUnordered<_>>();
		queue.try_for_each(|()| async { Ok(()) }).await?;

		// Wait for evicts to finish.
		self.wait_all_evict().await;

		// Now flush the object list and bitmap.
		[OBJECT_LIST_ID, OBJECT_BITMAP_ID]
			.into_iter()
			.map(|id| flush_object(id, RootIndex::I0))
			.collect::<FuturesUnordered<_>>()
			.try_for_each(|()| async { Ok(()) })
			.await?;

		// Wait for evicts to finish.
		self.wait_all_evict().await;

		// Tadha!
		debug_assert!(self.data().dirty.is_empty(), "flush_all didn't flush all");
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
