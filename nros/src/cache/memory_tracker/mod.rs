mod lru;
mod total_usage;

use {
	super::{entry::LruRef, IdKey},
	crate::{waker_queue, Dev, MaxRecordSize, Resource, WakerQueue, WakerQueueTicket},
	core::task::Waker,
};

pub use lru::Idx;

/// Estimated fixed cost for every cached entry.
///
/// This is in addition to the amount of data stored by the entry.
pub(super) const ENTRY_COST: usize = 32;

/// Memory usage tracking.
#[derive(Debug)]
pub(super) struct MemoryTracker {
	/// Total usage tracker.
	total: total_usage::TotalMemoryUsage,
	/// LRU tracker for unreferenced entries.
	lru: lru::Lru,
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

	/// Begin tracking memory usage of data that will be fetched.
	///
	/// Fails if the hard limit would be exceeded.
	///
	/// On failure, adds the waker to a queue and returns an object tracking
	/// the queue state.
	#[must_use = "return value must be checked"]
	fn start_fetch(&mut self, size: usize, waker: &Waker) -> Option<WakerQueueTicket<usize>> {
		self.total.add(size, waker)
	}

	/// Mark fetch as finished.
	///
	/// Must be used together with [`Self::start_fetch`].
	/// The size argument *must* be exactly the same.
	///
	/// This wakes all tasks waiting on the busy entry.
	#[must_use]
	fn soft_add(&mut self, key: IdKey, refcount: usize, size: usize) -> LruRef {
		trace!("MemoryTracker::soft_add {:?} {}", refcount, size);
		if refcount > 0 {
			LruRef::Ref { refcount }
		} else {
			let lru_index = self.lru.add(key, size);
			trace!(info "add {:?}", lru_index);
			self.soft_wakers.wake_all();
			LruRef::NoRef { lru_index }
		}
	}

	/// Reserve memory even if the limit would be exceeded.
	///
	/// If the limit is exceeded, the calling task should yield as soon as possible
	/// with [`Cache::memory_compensate_forced_grow`].
	pub fn force_grow(&mut self, refcount: &LruRef, old_size: usize, new_size: usize) {
		trace!("force_grow {} {}", old_size, new_size);
		self.total.force_add(new_size - old_size);
		if matches!(refcount, LruRef::NoRef { .. }) {
			self.lru.adjust(old_size, new_size);
		}
	}

	/// Decrease the memory usage of already present data.
	///
	/// This always succeeds immediately.
	pub fn shrink(&mut self, refcount: &LruRef, old_size: usize, new_size: usize) {
		trace!("shrink {:?} {} -> {}", refcount, old_size, new_size);
		debug_assert!(old_size >= new_size);
		self.total.remove(old_size - new_size);
		if matches!(refcount, LruRef::NoRef { .. }) {
			self.lru.adjust(old_size, new_size);
			self.soft_wakers.wake_all();
		}
	}

	/// Decrease reference count to data by `count`.
	///
	/// # Panics
	///
	/// If there are no live references.
	fn decr_refcount(&mut self, key: IdKey, refcount: &mut LruRef, size: usize, count: usize) {
		trace!("decr_refcount {:?} {} -{}", refcount, size, count);
		let LruRef::Ref { refcount: c } = refcount else { panic!("no live references") };
		*c -= count;
		if *c == 0 {
			let lru_index = self.lru.add(key, size);
			trace!(info "add {:?}", lru_index);
			*refcount = LruRef::NoRef { lru_index };
			self.soft_wakers.wake_all();
		}
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

	/// Stop tracking *soft* usage of data.
	#[must_use]
	fn soft_remove(&mut self, refcount: &LruRef, size: usize) -> usize {
		trace!("soft_remove {:?} {}", refcount, size);
		match refcount {
			&LruRef::Ref { refcount } => refcount,
			&LruRef::NoRef { lru_index } => {
				self.lru.remove(lru_index, size);
				0
			}
		}
	}

	/// Stop tracking *hard* usage of data.
	///
	/// Must be used in combination with [`Self::soft_remove`].
	fn hard_remove(&mut self, size: usize) {
		self.total.remove(size);
	}

	/// Mark data as touched.
	pub fn touch(&mut self, refcount: &LruRef) {
		if let LruRef::NoRef { lru_index } = refcount {
			self.lru.touch(*lru_index);
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

/// Entry-specific impl
impl MemoryTracker {
	/// Increase reference count to entry by `count`.
	///
	/// # Panics
	///
	/// If there are no live references.
	pub(crate) fn decr_entry_refcount(
		&mut self,
		key: IdKey,
		refcount: &mut LruRef,
		length: usize,
		count: usize,
	) {
		self.decr_refcount(key, refcount, ENTRY_COST + length, count)
	}
}

impl<D: Dev, R: Resource> super::Cache<D, R> {
	/// Reserve the given amount of memory.
	///
	/// If it is not immediately available, this function will block.
	pub(super) async fn memory_reserve(&self, size: usize) {
		trace!("memory_reserve {}", size);
		waker_queue::poll(move |cx| {
			let mut data = self.data.borrow_mut();
			match data.memory_tracker.start_fetch(size, cx.waker()) {
				Some(t) => Err(t),
				None => Ok(()),
			}
		})
		.await
	}

	/// Reserve the given amount of memory for an entry.
	///
	/// If it is not immediately available, this function will block.
	pub(super) async fn memory_reserve_entry(&self, size: usize) {
		self.memory_reserve(ENTRY_COST + size).await
	}

	/// Wait until the total memory usage drops below the limit.
	///
	/// If it is not immediately available, this function will block.
	pub(super) async fn memory_compensate_forced_grow(&self) {
		self.memory_reserve(0).await
	}

	#[must_use]
	pub(super) fn memory_soft_add_entry(
		&self,
		key: IdKey,
		refcount: usize,
		length: usize,
	) -> LruRef {
		self.data
			.borrow_mut()
			.memory_tracker
			.soft_add(key, refcount, ENTRY_COST + length)
	}

	#[must_use]
	pub(super) fn memory_soft_remove_entry(&self, refcount: LruRef, length: usize) -> usize {
		self.data
			.borrow_mut()
			.memory_tracker
			.soft_remove(&refcount, ENTRY_COST + length)
	}

	pub(super) fn memory_hard_shrink(&self, amount: usize) {
		self.data.borrow_mut().memory_tracker.total.remove(amount);
	}

	/// Stop tracking *hard* usage of an entry.
	///
	/// Must be used in combination with [`Self::soft_remove_entry`].
	pub(super) fn memory_hard_remove_entry(&self, length: usize) {
		self.data
			.borrow_mut()
			.memory_tracker
			.hard_remove(ENTRY_COST + length)
	}

	/// Check if cache size matches real usage
	#[track_caller]
	pub(crate) fn verify_cache_usage(&self) {
		if !(cfg!(test) || cfg!(fuzzing)) {
			return;
		}
		use super::{Buf, Entry};
		let data = &mut *self.data.borrow_mut();
		let real_usage = data.objects.values_mut().fold(0, |x, obj| {
			x + obj
				.records
				.values()
				.flat_map(|s| match s {
					Entry { data, lru_ref: LruRef::NoRef { .. } } => Some(data),
					_ => None,
				})
				.fold(0, |x, v| x + v.len() + ENTRY_COST)
		});
		assert_eq!(
			real_usage,
			data.memory_tracker.lru.size(),
			"cache size mismatch"
		);
		assert!(
			data.memory_tracker.lru.size() <= data.memory_tracker.total.usage(),
			"lru size larger than total usage"
		);
	}
}
