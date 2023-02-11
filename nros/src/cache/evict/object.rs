use {
	super::super::{
		Cache, Present, Slot, SlotExt, TreeData, ID_PSEUDO, OBJECT_BITMAP_ID, OBJECT_LIST_ID,
	},
	crate::{
		util::{self, BTreeMapExt},
		Dev, Error, Resource,
	},
	core::{future::Future, pin::Pin},
};

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Evict an object from the cache.
	pub(super) fn evict_object(
		&self,
		id: u64,
	) -> Option<Pin<Box<dyn Future<Output = Result<(), Error<D>>> + '_>>> {
		trace!("evict_object {:#x}", id);

		// Get object
		let data = &mut *self.data.borrow_mut();
		let mut obj_entry = data.objects.occupied(id).expect("no object");
		let obj = obj_entry.as_present_mut().expect("no object");

		debug_assert!(
			obj.data().iter().all(|lvl| lvl.slots.is_empty()),
			"object still has entries"
		);

		// Remove from memory tracker
		let busy = data.memory_tracker.soft_remove_object(&obj.refcount);

		// Take object field
		let object = obj.data.object();

		let fut = if id == OBJECT_LIST_ID {
			// Just copy
			trace!(info "object list");
			self.store.set_object_list_root(object.root);
			None
		} else if id == OBJECT_BITMAP_ID {
			// Just copy
			trace!(info "object bitmap");
			self.store.set_object_bitmap_root(object.root);
			None
		} else if super::super::is_pseudo_id(id) {
			// Just discard
			// Pseudo-object should be all zeroes on evict.
			trace!(info "pseudo object");
			debug_assert_eq!(object.root.length(), 0, "pseudo object leaks entries");
			None
		} else if !obj.data.is_dirty() {
			// Just remove
			trace!(info "not dirty");
			None
		} else {
			*obj_entry.get_mut() = Slot::Busy(busy.clone());

			data.evict_tasks_count += 1;
			Some(util::box_fut(async move {
				trace!("evict_object::(background) {:#x}", id);

				// Only remove the object from hard tracking now to "trick" the main task
				// into yielding.
				self.data.borrow_mut().memory_tracker.hard_remove_object();

				self.save_object(id, &object).await?;

				let busy_ref = busy.borrow_mut();
				debug_assert_eq!(busy_ref.key.id(), id, "id of object changed while evicting");
				let referenced = busy_ref.refcount > 0;
				drop(busy_ref);

				if referenced {
					// Reserve memory for reinsertion.
					self.memory_reserve_object().await;
				}

				let data = &mut *self.data.borrow_mut();
				let mut slot = data.objects.occupied(id).expect("no object");

				if referenced {
					trace!(info "keep");
					*slot.get_mut() = Slot::Present(Present {
						refcount: data.memory_tracker.finish_fetch_object(busy),
						data: TreeData::new(object, self.max_record_size()),
					});
				} else {
					trace!(info "remove");
					busy.borrow_mut().wake_all();
					slot.remove();
				}

				data.evict_tasks_count -= 1;
				if data.evict_tasks_count == 0 {
					data.wake_after_evicts.take().map(|w| w.wake());
				}

				Ok(())
			}))
		};

		if fut.is_none() {
			obj_entry.remove();
			data.memory_tracker.hard_remove_object();
		}

		fut
	}
}
