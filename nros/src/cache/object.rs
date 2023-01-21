use {
	super::{Busy, Cache, Dev, Lru, Present, Resource, Slot, TreeData},
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
	) -> Option<(RefMut<'_, Present<TreeData<R>>>, RefMut<'_, Lru>)> {
		trace!("wait_object {:#x}", id);
		let mut busy = None::<Rc<RefCell<Busy>>>;
		future::poll_fn(|cx| {
			if let Some(busy) = busy.as_mut() {
				id = busy.borrow_mut().key.id();
			}

			let data = self.data.borrow_mut();
			let (objects, mut lru) = RefMut::map_split(data, |d| (&mut d.objects, &mut d.lru));
			let obj = RefMut::filter_map(objects, |objects| match objects.get_mut(&id) {
				Some(Slot::Present(obj)) => {
					if busy.is_some() {
						lru.object_decrease_refcount(id, &mut obj.refcount, 1);
					}
					Some(obj)
				}
				Some(Slot::Busy(obj)) => {
					let mut e = obj.borrow_mut();
					e.wakers.push(cx.waker().clone());
					if busy.is_none() {
						e.refcount += 1;
						busy = Some(obj.clone());
					}
					None
				}
				None => {
					debug_assert!(busy.is_none(), "object removed while waiting");
					None
				}
			});
			match obj {
				Ok(obj) => Poll::Ready(Some((obj, lru))),
				Err(_) if busy.is_some() => Poll::Pending,
				Err(_) => Poll::Ready(None),
			}
		})
		.await
	}
}
