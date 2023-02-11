use {
	super::{memory_tracker::Idx, Key},
	crate::waker_queue::WakerQueue,
	alloc::{collections::btree_map, rc::Rc},
	core::cell::RefCell,
};

/// A single slot with cached data.
#[derive(Debug)]
pub(super) enum Slot<T> {
	/// The entry is present.
	Present(Present<T>),
	/// The entry is being fetched or flushed.
	Busy(Rc<RefCell<Busy>>),
}

impl<T> Slot<T> {
	pub fn as_present_mut(&mut self) -> Option<&mut Present<T>> {
		match self {
			Self::Present(e) => Some(e),
			_ => None,
		}
	}

	fn as_busy(&self) -> Option<&Rc<RefCell<Busy>>> {
		match self {
			Self::Busy(e) => Some(e),
			_ => None,
		}
	}
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
	pub wakers: WakerQueue<()>,
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
		Rc::new(RefCell::new(Self {
			wakers: Default::default(),
			refcount: 0,
			key,
		}))
	}

	pub fn with_refcount(key: Key, refcount: usize) -> Rc<RefCell<Self>> {
		Rc::new(RefCell::new(Self {
			wakers: Default::default(),
			refcount,
			key,
		}))
	}

	pub fn wake_all(&mut self) {
		self.wakers.wake_all();
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
		lru_index: Idx,
	},
}

pub(super) trait SlotExt<'a, T> {
	fn into_present_mut(self) -> Option<&'a mut Present<T>>;

	fn as_present_mut(&mut self) -> Option<&mut Present<T>>;

	fn as_busy(&self) -> Option<&Rc<RefCell<Busy>>>;
}

impl<'a, T> SlotExt<'a, T> for btree_map::Entry<'a, u64, Slot<T>> {
	fn into_present_mut(self) -> Option<&'a mut Present<T>> {
		let btree_map::Entry::Occupied(e) = self else { return None };
		e.into_mut().as_present_mut()
	}

	fn as_present_mut(&mut self) -> Option<&mut Present<T>> {
		let btree_map::Entry::Occupied(e) = self else { return None };
		e.get_mut().as_present_mut()
	}

	fn as_busy(&self) -> Option<&Rc<RefCell<Busy>>> {
		let btree_map::Entry::Occupied(e) = self else { return None };
		e.get().as_busy()
	}
}

impl<'a, T> SlotExt<'a, T> for btree_map::OccupiedEntry<'a, u64, Slot<T>> {
	fn into_present_mut(self) -> Option<&'a mut Present<T>> {
		let Slot::Present(p) = self.into_mut() else { return None };
		Some(p)
	}

	fn as_present_mut(&mut self) -> Option<&mut Present<T>> {
		self.get_mut().as_present_mut()
	}

	fn as_busy(&self) -> Option<&Rc<RefCell<Busy>>> {
		self.get().as_busy()
	}
}

impl<'a, T> SlotExt<'a, T> for Option<&'a mut Slot<T>> {
	fn into_present_mut(self) -> Option<&'a mut Present<T>> {
		let Slot::Present(p) = self? else { return None };
		Some(p)
	}

	fn as_present_mut(&mut self) -> Option<&mut Present<T>> {
		self.as_mut()?.as_present_mut()
	}

	fn as_busy(&self) -> Option<&Rc<RefCell<Busy>>> {
		self.as_ref()?.as_busy()
	}
}
