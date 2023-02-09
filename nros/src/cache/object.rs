use {
	super::{
		Buf, Busy, Cache, Dev, Error, MemoryTracker, Object, Present, Resource, Slot, Tree,
		TreeData, OBJECT_BITMAP_FIELD_RATIO_P2, OBJECT_BITMAP_ID, OBJECT_BITMAP_INUSE,
		OBJECT_BITMAP_NONZERO, OBJECT_LIST_ID, OBJECT_SIZE_P2,
	},
	crate::{util, waker_queue},
	alloc::rc::Rc,
	core::{
		cell::{RefCell, RefMut},
		future,
		task::Poll,
	},
};

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Try to get an object directly.
	///
	/// This will block if a task is busy with the object.
	pub(super) async fn wait_object(
		&self,
		mut id: u64,
	) -> Option<(
		RefMut<'_, Present<TreeData<R::Buf>>>,
		RefMut<'_, MemoryTracker>,
	)> {
		trace!("wait_object {:#x}", id);
		let mut busy = None::<Rc<RefCell<Busy>>>;
		waker_queue::poll(|cx| {
			if let Some(busy) = busy.as_mut() {
				id = busy.borrow_mut().key.id();
			}

			let data = self.data.borrow_mut();
			let (objects, mut memory_tracker) =
				RefMut::map_split(data, |d| (&mut d.objects, &mut d.memory_tracker));
			let mut ticket = None;
			let obj = RefMut::filter_map(objects, |objects| match objects.get_mut(&id) {
				Some(Slot::Present(obj)) => {
					if busy.is_some() {
						memory_tracker.decr_object_refcount(&mut obj.refcount, 1);
					}
					Some(obj)
				}
				Some(Slot::Busy(obj)) => {
					let mut e = obj.borrow_mut();
					ticket = Some(e.wakers.push(cx.waker().clone(), ()));
					if busy.is_none() {
						e.refcount += 1;
					}
					busy = Some(obj.clone());
					None
				}
				None => {
					debug_assert!(busy.is_none(), "object removed while waiting");
					None
				}
			});
			if let Some(ticket) = ticket {
				Err(ticket)
			} else if let Ok(obj) = obj {
				#[cfg(debug_assertions)]
				obj.check_integrity();
				Ok(Some((obj, memory_tracker)))
			} else {
				Ok(None)
			}
		})
		.await
	}

	/// Try to get an object directly.
	pub(super) fn get_object(
		&self,
		id: u64,
	) -> Option<(
		RefMut<'_, Present<TreeData<R::Buf>>>,
		RefMut<'_, MemoryTracker>,
	)> {
		let data = self.data.borrow_mut();
		let (objects, lru) = RefMut::map_split(data, |d| (&mut d.objects, &mut d.memory_tracker));
		let object = RefMut::filter_map(objects, |o| match o.get_mut(&id) {
			Some(Slot::Present(slot)) => Some(slot),
			_ => None,
		});
		Some((object.ok()?, lru))
	}

	/// Save an object.
	pub(super) async fn save_object<'a>(
		&'a self,
		id: u64,
		object: &Object,
	) -> Result<(), Error<D>> {
		trace!("save_object {:#x} {:?}", id, object);
		debug_assert!(id < 1 << 58, "id out of range");

		// Write to list
		let (offt, index) = util::divmod_p2(id << OBJECT_SIZE_P2, self.max_record_size().to_raw());
		Tree::new(self, OBJECT_LIST_ID)
			.get(0, offt)
			.await?
			.write(index, object.as_ref())
			.await;

		// Write to bitmap
		let mut bits = 0;
		(object.reference_count != 0).then(|| bits |= OBJECT_BITMAP_INUSE);
		(object.root.length() != 0).then(|| bits |= OBJECT_BITMAP_NONZERO);

		let (offt, shift) = util::divmod_p2(id, 2);
		let (offt, index) = util::divmod_p2(offt, self.max_record_size().to_raw());
		let entry = Tree::new(self, OBJECT_BITMAP_ID).get(0, offt).await?;
		let byte = &mut [0];
		util::read(index, byte, entry.get());
		byte[0] &= !(3 << shift * 2);
		byte[0] |= bits << shift * 2;
		entry.write(index, byte).await;

		Ok(())
	}

	/// Grow the object list, i.e. add one level.
	pub(super) async fn grow_object_list<'a>(&'a self) -> Result<(), Error<D>> {
		trace!("grow_object_list");

		let bytelen = self.object_list_bytelen();
		let bytelen = if bytelen == 0 {
			1 << self.max_record_size().to_raw()
		} else {
			bytelen << self.entries_per_parent_p2()
		};
		let bitmap_len = bytelen >> OBJECT_BITMAP_FIELD_RATIO_P2;

		let list = Tree::new(self, OBJECT_LIST_ID);
		let bitmap = Tree::new(self, OBJECT_BITMAP_ID);

		futures_util::try_join!(list.resize(bytelen), bitmap.resize(bitmap_len))?;

		self.store
			.set_object_list_depth(self.store.object_list_depth() + 1);

		Ok(())
	}
}
