use {
	super::{lru, Key},
	alloc::rc::Rc,
	core::{cell::RefCell, task::Waker},
};

/// A single slot with cached data.
#[derive(Debug)]
pub(super) enum Slot<T> {
	/// The entry is present.
	Present(Present<T>),
	/// The entry is being fetched or flushed.
	Busy(Rc<RefCell<Busy>>),
}

#[derive(Debug)]
pub(super) struct Present<T> {
	/// The cached data.
	pub data: T,
	/// The amount of tasks referencing this data.
	pub refcount: RefCount,
}

#[derive(Debug)]
pub(super) struct Busy {
	/// Wakers for tasks waiting for this slot.
	pub wakers: Vec<Waker>,
	/// The amount of tasks referencing this data.
	///
	/// *Excludes* the busy task.
	pub refcount: usize,
	/// The key the task uses to locate this slot.
	///
	/// This must be updated if the slot is moved.
	pub key: Key,
}

impl Busy {
	pub fn new(key: Key) -> Rc<RefCell<Self>> {
		Rc::new(RefCell::new(Self { wakers: vec![], refcount: 0, key }))
	}

	pub fn with_refcount(key: Key, refcount: usize) -> Rc<RefCell<Self>> {
		Rc::new(RefCell::new(Self { wakers: vec![], refcount, key }))
	}
}

/// Reference counter to prevent slots from transitioning to
/// empty or flushing state before all tasks have finished with it.
#[derive(Debug)]
pub(super) enum RefCount {
	/// There are tasks remaining.
	Ref {
		/// Reference to a [`Busy`] object shared with tasks waiting on this slot.
		busy: Rc<RefCell<Busy>>,
	},
	/// There are no remaining entries.
	NoRef {
		/// The position in the LRU.
		lru_index: lru::Idx,
	},
}
