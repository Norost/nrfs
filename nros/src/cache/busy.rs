use {
	super::IdKey,
	crate::{waker_queue::WakerQueue, Dev, Resource},
};

#[derive(Debug, Default)]
pub(super) struct Busy {
	/// Wakers for tasks waiting for this slot.
	pub wakers: WakerQueue<()>,
	/// The amount of tasks referencing this data.
	///
	/// *Excludes* the busy task.
	pub refcount: usize,
}

impl<D: Dev, R: Resource> super::Cache<D, R> {
	/// # Panics
	///
	/// If already busy.
	pub(super) fn busy_insert(&self, key: IdKey, refcount: usize) {
		let mut data = self.data.borrow_mut();
		let busy = Busy { wakers: Default::default(), refcount };
		let prev = data.busy.insert(key, busy);
		assert!(prev.is_none(), "already busy");
	}

	/// # Panics
	///
	/// If not busy.
	#[must_use = "refcount"]
	pub(super) fn busy_remove(&self, key: IdKey) -> usize {
		let mut data = self.data.borrow_mut();
		let mut busy = data.busy.remove(&key).expect("not busy");
		busy.wakers.wake_all();
		busy.refcount
	}
}
