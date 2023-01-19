use {
	super::lru,
	crate::Resource,
	core::{num::NonZeroUsize, task::Waker},
};

/// A single slot with cached data.
#[derive(Debug)]
pub enum Slot<T> {
	/// The entry is present.
	Present(Present<T>),
	/// The entry is being fetched or flushed.
	Busy(Busy),
}

#[derive(Debug)]
pub struct Present<T> {
	/// The cached data.
	pub data: T,
	/// The amount of tasks referencing this data.
	pub refcount: RefCount,
}

#[derive(Debug, Default)]
pub struct Busy {
	/// Wakers for tasks waiting for this slot.
	pub wakers: Vec<Waker>,
	/// The amount of tasks referencing this data.
	pub refcount: Option<NonZeroUsize>,
}

/// Reference counter to prevent slots from transitioning to
/// empty or flushing state before all tasks have finished with it.
#[derive(Debug)]
pub enum RefCount {
	/// There are tasks remaining.
	Ref {
		/// The amount of tasks waiting.
		count: NonZeroUsize,
	},
	/// There are no remaining entries.
	NoRef {
		/// The position in the LRU.
		lru_index: lru::Idx,
	},
}

impl RefCount {
	pub(super) fn busy_to_present(
		refcount: Option<NonZeroUsize>,
		lru: &mut super::Lru,
		key: super::Key,
		len: usize,
	) -> Self {
		match refcount {
			Some(count) => Self::Ref { count },
			None => {
				let lru_index = lru.add(key, len);
				Self::NoRef { lru_index }
			}
		}
	}

	pub(super) fn present_to_busy(&self, lru: &mut super::Lru, len: usize) -> Option<NonZeroUsize> {
		match self {
			Self::Ref { count } => Some(*count),
			Self::NoRef { lru_index } => {
				lru.remove(*lru_index, len);
				None
			}
		}
	}
}
