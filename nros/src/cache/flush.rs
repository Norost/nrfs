use {
	super::{
		is_pseudo_id, Busy, Cache, EntryRef, Key, Present, Slot, SlotExt, Tree, OBJECT_BITMAP_ID,
		OBJECT_LIST_ID,
	},
	crate::{resource::Buf, waker_queue, Dev, Error, Resource},
	alloc::rc::Rc,
	core::{
		cell::{RefCell, RefMut},
		future, mem,
		task::Poll,
	},
	futures_util::stream::{FuturesUnordered, TryStreamExt},
};

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Wait for an entry to not be busy.
	///
	/// Unlike [`wait_entry`] this will not cause the entry to be refetched if evicted.
	async fn wait_entry_nofetch(&self, mut key: Key) -> Option<EntryRef<'_, D, R>> {
		trace!("wait_entry_nofetch {:?}", key);
		let mut busy = None::<Rc<RefCell<Busy>>>;
		waker_queue::poll(|cx| {
			if let Some(busy) = busy.as_mut() {
				key = busy.borrow_mut().key;
			}

			let Some(c) = self.get_entryref_components(key)
				else { return Ok(None) };

			let mut ticket = None;
			let Ok(entry) = RefMut::filter_map(c.slot, |slot| match slot {
				Slot::Present(p) => Some(p),
				Slot::Busy(b) => {
					// Do *not* increase refcount to avoid having the entry being fetched again.
					if busy.is_none() {
						busy = Some(b.clone());
					}
					ticket = Some(b.borrow_mut().wakers.push(cx.waker().clone(), ()));
					None
				}
			}) else { return Err(ticket.unwrap()) };

			let entry = EntryRef::new(self, key, entry, c.dirty_markers, c.memory_tracker);
			Ok(Some(entry))
		})
		.await
	}

	/// Flush an entry from the cache.
	///
	/// This does not evict the entry.
	///
	/// Does nothing if the entry wasn't present or dirty.
	async fn flush_entry(&self, key: Key) -> Result<(), Error<D>> {
		trace!("flush_entry {:?}", key);

		// Wait for entry
		// Don't use wait_entry as that may redundantly fetch the entry again.
		let Some(entry) = self.wait_entry_nofetch(key).await else { return Ok(()) };

		if !entry.dirty_markers.contains_key(&key.offset()) {
			trace!(info "not dirty");
			// The entry is not dirty, so skip.
			return Ok(());
		}

		// Take entry.
		drop(entry);
		let mut data = self.data.borrow_mut();
		let data_ref = &mut *data;
		let obj = data_ref
			.objects
			.get_mut(&key.id())
			.into_present_mut()
			.expect("no object");
		let slot = obj.get_mut(key.depth(), key.offset()).expect("no entry");
		let present = slot.as_present_mut().expect("no entry");

		let entry = mem::replace(&mut present.data, self.resource().alloc());
		let busy = data_ref
			.memory_tracker
			.soft_remove_entry(&present.refcount, entry.len());
		*slot = Slot::Busy(busy.clone());

		// Unmark as dirty
		let r = obj
			.data
			.unmark_dirty(key.depth(), key.offset(), self.max_record_size());
		debug_assert!(r, "not marked");

		// Store entry.
		drop(data);
		let (rec, entry) = self.store.write(entry).await?;
		let key = busy.borrow_mut().key;

		// TODO check if tree can allocate enough reserved memory to operate
		// If not, discard entry to avoid potential deadlock.
		// TODO check LRU too.
		// FIXME don't just fucking discard goddamn

		let entry_len = entry.len();
		let entry = if false {
			Some(entry)
		} else {
			// Drop entry data for now.
			let data = &mut *self.data.borrow_mut();
			data.memory_tracker.hard_remove_entry(entry.len());
			drop(entry);
			None
		};

		Tree::new(self, key.id())
			.update_record(key.depth(), key.offset(), rec, &busy)
			.await?;

		// Fetch entry again if a task needs it.
		let (key, entry) = if let Some(entry) = entry {
			(key, Some(entry))
		} else if busy.borrow_mut().refcount > 0 {
			self.memory_reserve_entry(entry_len).await;
			let entry = self.store.read(&rec).await?;
			(busy.borrow_mut().key, Some(entry))
		} else {
			(key, None)
		};

		let data = &mut *self.data.borrow_mut();
		let obj = data
			.objects
			.get_mut(&key.id())
			.into_present_mut()
			.expect("no object");
		let mut slot = obj.occupied(key.depth(), key.offset()).expect("no entry");
		debug_assert!(matches!(slot.get(), Slot::Busy(_)), "entry not busy");

		if let Some(entry) = entry {
			// Put entry back in.
			let refcount = data.memory_tracker.finish_fetch_entry(busy, entry.len());
			*slot.get_mut() = Slot::Present(Present { data: entry, refcount });
		} else {
			// Remove busy slot.
			slot.remove();
		}

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
			for d in 0.. {
				// If None, it has been evicted and nothing is left for us to do.
				let Some((mut obj, _)) = self.wait_object(id).await else { return Ok(()) };
				if d >= u8::try_from(obj.data().len()).unwrap() {
					// Highest level has been flushed.
					break;
				}
				// Collect all offsets
				let mut offt = obj
					.level_mut(d)
					.dirty_markers
					.keys()
					.copied()
					.collect::<Vec<_>>();
				// Sort to take better advantage of caching.
				offt.sort_unstable();
				// Flush all entries at current level.
				drop(obj);
				offt.into_iter()
					.map(|o| self.flush_entry(Key::new(0, id, d, o)))
					.collect::<FuturesUnordered<_>>()
					.try_for_each(|()| async { Ok(()) })
					.await?;
			}
			Ok(())
		};
		#[cfg(feature = "trace")]
		let flush_object = |id| crate::trace::TracedTask::new(flush_object(id));

		let data = || self.data.borrow_mut();

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

		// Write object roots.
		let mut ids = data()
			.objects
			.iter()
			.filter(|&(&id, obj)| {
				let Slot::Present(obj) = obj else { unreachable!() };
				id & (3 << 58) == 0 && obj.data.is_dirty()
			})
			.map(|(id, _)| *id)
			.collect::<Vec<_>>();

		// Sort the objects to take better advantage of caching.
		ids.sort_unstable();

		let queue = ids
			.into_iter()
			.filter(|&id| !is_pseudo_id(id))
			.map(|id| async move {
				trace!("flush_all::save_object {:#x}", id);
				let Some((mut obj, _)) = self.wait_object(id).await else { return Ok(()) };
				if obj.data.is_dirty() {
					let object = obj.data.object();
					obj.data.clear_dirty();
					drop(obj);
					self.save_object(id, &object).await?;
				}
				Ok(())
			});
		#[cfg(feature = "trace")]
		let queue = queue.map(crate::trace::TracedTask::new);
		queue
			.collect::<FuturesUnordered<_>>()
			.try_for_each(|()| async { Ok(()) })
			.await?;

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

		let data = self.data.borrow_mut();

		// Write object list root.
		if let Some(Slot::Present(obj)) = data.objects.get(&OBJECT_LIST_ID) {
			self.store.set_object_list_root(obj.data.object().root);
		}

		// Write object bitmap root.
		if let Some(Slot::Present(obj)) = data.objects.get(&OBJECT_BITMAP_ID) {
			self.store.set_object_bitmap_root(obj.data.object().root);
		}

		// Tadha!
		// Do a sanity check just in case.
		if cfg!(debug_assertions) {
			for (&id, tree) in data.objects.iter() {
				let Slot::Present(tree) = tree else { unreachable!() };

				for level in tree.data().iter() {
					debug_assert!(level.dirty_markers.is_empty(), "flush_all didn't flush all");
				}

				if is_pseudo_id(id) {
					debug_assert_eq!(
						tree.data.object().root.length(),
						0,
						"pseudo object is not zero"
					);
				}
			}
		}
		Ok(())
	}

	async fn wait_all_evict(&self) {
		trace!("wait_all_evict");
		future::poll_fn(|cx| {
			let mut data = self.data.borrow_mut();
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
