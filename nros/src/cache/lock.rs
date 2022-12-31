use {
	super::{Cache, CacheData, Dev, Key, Record},
	core::{
		cell::RefCell,
		future,
		task::{Poll, Waker},
	},
	rustc_hash::FxHashMap,
	std::collections::hash_map,
};

/// An active resize lock.
#[derive(Debug)]
pub struct ResizeLock {
	/// The new length of the tree being resized.
	pub new_len: u64,
	/// Records of subtrees that have been flushed but should still be destroyed.
	pub destroy_records: FxHashMap<(u8, u64), Record>,
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
					e.insert(ResizeLock {
						new_len,
						destroy_records: Default::default(),
						wakers: Default::default(),
					});
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

		if cfg!(debug_assertions) && !std::thread::panicking() {
			let data = self.data.borrow_mut();
			let slf = data.resizing.get(&self.id).unwrap();
			assert!(
				slf.destroy_records.is_empty(),
				"not all records were destroyed\n{:#?}",
				&slf.destroy_records
			);
		}

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
