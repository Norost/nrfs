mod lru;
mod total_usage;

use {
	super::super::IdKey,
	crate::{waker_queue, Dev, MaxRecordSize, Resource, WakerQueue, WakerQueueTicket},
	core::{cell::RefMut, task::Waker},
};

pub use lru::{Idx, IDX_NONE};

/// Estimated fixed cost for every cached entry.
///
/// This is in addition to the amount of data stored by the entry.
pub(super) const ENTRY_COST: usize = 32;

/// Memory usage tracking.
#[derive(Debug)]
pub(super) struct MemoryTracker {
	/// Total usage tracker.
	pub(super) total: total_usage::TotalMemoryUsage,
	/// LRU tracker for unreferenced entries.
	pub(super) lru: lru::Lru,
	/// Tasks to wake if there are more entries to evict.
	soft_wakers: WakerQueue<()>,
}

impl MemoryTracker {
	/// Create a new memory usage tracker.
	///
	/// The soft limit indicates the size of memory that will be kept cached at all times.
	/// It may be exceeded during operations.
	///
	/// The hard limit is *never* exceeded.
	/// Operations will block if this limit would be exceeded.
	pub fn new(soft_limit: usize, hard_limit: usize) -> Self {
		Self {
			total: total_usage::TotalMemoryUsage::new(hard_limit),
			lru: lru::Lru::new(soft_limit),
			soft_wakers: Default::default(),
		}
	}

	#[must_use = "return value must be checked"]
	pub fn hard_add(&mut self, size: usize, waker: &Waker) -> Option<WakerQueueTicket<usize>> {
		self.total.add(size, waker)
	}

	pub fn hard_del(&mut self, size: usize) {
		self.total.remove(size);
	}

	pub fn soft_add(&mut self, key: IdKey, size: usize) -> Idx {
		self.lru.add(key, size)
	}

	pub fn soft_del(&mut self, idx: Idx, size: usize) -> IdKey {
		self.lru.remove(idx, size)
	}

	/// Get the next key of data to evict.
	///
	/// Returns a handle which can be used to stop tracking memory usage of the data.
	///
	/// Returns `None` if no data needs to be evicted.
	///
	/// # Note
	///
	/// Do not use [`incr_refcount`] while operating on the corresponding data.
	pub fn evict_next(
		&mut self,
		max_record_size: MaxRecordSize,
		waker: &Waker,
	) -> Result<(IdKey, Idx), WakerQueueTicket<()>> {
		let mut do_evict = self.lru.has_excess();

		// If there is not enough room to fetch even one entry, evict even if the LRU disagrees.
		do_evict |= !self.total.has_room_for_entry(max_record_size);

		if let Some(v) = do_evict.then(|| self.lru.last()).flatten() {
			Ok(v)
		} else {
			Err(self.soft_wakers.push(waker.clone(), ()))
		}
	}

	/// Mark data as touched.
	pub fn touch(&mut self, idx: Idx) {
		if idx != IDX_NONE {
			self.lru.touch(idx);
		}
	}

	/// Amount of bytes counting towards the soft limit.
	pub fn soft_usage(&self) -> usize {
		self.lru.size()
	}

	/// Amount of bytes counting towards the hard limit.
	pub fn hard_usage(&self) -> usize {
		self.total.usage()
	}

	/// Set soft limit.
	pub fn set_soft_limit(&mut self, size: usize) {
		self.lru.set_cache_max(size)
	}
}
