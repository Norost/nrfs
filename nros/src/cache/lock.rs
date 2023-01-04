use {
	super::{Cache, CacheData, Dev},
	core::{
		cell::RefCell,
		future,
		task::{Poll, Waker},
	},
	std::collections::hash_map,
};

/// An active resize lock.
#[derive(Debug)]
pub struct ResizeLock {
	/// The new length of the tree being resized.
	pub new_len: u64,
	/// Tasks to wake when this lock is released.
	wakers: Vec<Waker>,
}

impl<D: Dev> Cache<D> {
	/// Lock an object for resizing.
	pub(super) async fn lock_resizing(&self, id: u64, new_len: u64) -> ResizeGuard<'_> {
		trace!("lock_resizing");
		future::poll_fn(move |cx| {
			let mut data = self.data.borrow_mut();
			match data.resizing.entry(id) {
				hash_map::Entry::Vacant(e) => {
					// Acquire lock.
					e.insert(ResizeLock { new_len, wakers: Default::default() });
					Poll::Ready(ResizeGuard { data: &self.data, id })
				}
				hash_map::Entry::Occupied(e) => {
					// Wait.
					e.into_mut().wakers.push(cx.waker().clone());
					Poll::Pending
				}
			}
		})
		.await
	}
}

/// A guard that blocks other resizes on trees while it is live.
pub struct ResizeGuard<'a> {
	data: &'a RefCell<CacheData>,
	id: u64,
}

impl Drop for ResizeGuard<'_> {
	fn drop(&mut self) {
		trace!("ResizeGuard::drop");

		// TODO waking literally every single one of them is inefficient.
		// It is easy though so w/e.
		self.data
			.borrow_mut()
			.resizing
			.remove(&self.id)
			.expect("no resizing lock")
			.wakers
			.into_iter()
			.for_each(|w| w.wake());
	}
}
