mod lru;
mod total_usage;

use {
	super::{Busy, Key, RefCount},
	crate::{waker_queue, Dev, MaxRecordSize, Resource, WakerQueue, WakerQueueTicket},
	alloc::rc::Rc,
	core::cell::RefCell,
	std::task::Waker,
};

pub use lru::Idx;

/// Estimated fixed cost for every cached entry.
///
/// This is in addition to the amount of data stored by the entry.
const ENTRY_COST: usize = 32;

/// Estimated fixed cost for every cached object.
const OBJECT_COST: usize = 128;

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
	fn finish_fetch(&mut self, busy: Rc<RefCell<Busy>>, size: usize) -> RefCount {
		let mut b = busy.borrow_mut();
		trace!("MemoryTracker::finish_fetch {:?} {}", &*b, size);
		b.wake_all();
		if b.refcount > 0 {
			drop(b);
			RefCount::Ref { busy }
		} else {
			let lru_index = self.lru.add(b.key, size);
			trace!(info "add {:?}", lru_index);
			self.soft_wakers.wake_all();
			RefCount::NoRef { lru_index }
		}
	}

	/// Reserve memory even if the limit would be exceeded.
	///
	/// If the limit is exceeded, the calling task should yield as soon as possible
	/// with [`Cache::memory_compensate_forced_grow`].
	pub fn force_grow(&mut self, refcount: &RefCount, old_size: usize, new_size: usize) {
		trace!("force_grow {} {}", old_size, new_size);
		self.total.force_add(new_size - old_size);
		if matches!(refcount, RefCount::NoRef { .. }) {
			self.lru.adjust(old_size, new_size);
		}
	}

	/// Decrease the memory usage of already present data.
	///
	/// This always succeeds immediately.
	pub fn shrink(&mut self, refcount: &RefCount, old_size: usize, new_size: usize) {
		trace!("shrink {:?} {} -> {}", refcount, old_size, new_size);
		debug_assert!(old_size >= new_size);
		self.total.remove(old_size - new_size);
		if matches!(refcount, RefCount::NoRef { .. }) {
			self.lru.adjust(old_size, new_size);
			self.soft_wakers.wake_all();
		}
	}

	/// Increase reference count to data by `count`.
	fn incr_refcount(&mut self, refcount: &mut RefCount, size: usize, count: usize) {
		trace!("incr_refcount {:?} {} +{}", refcount, size, count);
		match refcount {
			RefCount::Ref { busy } => busy.borrow_mut().refcount += count,
			RefCount::NoRef { lru_index } => {
				trace!(info "remove {:?}", lru_index);
				let key = self.lru.remove(*lru_index, size);
				let busy = Busy::with_refcount(key, count);
				*refcount = RefCount::Ref { busy };
			}
		}
	}

	/// Decrease reference count to data by `count`.
	///
	/// # Panics
	///
	/// If there are no live references.
	fn decr_refcount(&mut self, refcount: &mut RefCount, size: usize, count: usize) {
		trace!("decr_refcount {:?} {} -{}", refcount, size, count);
		let RefCount::Ref { busy } = refcount else { panic!("no live references") };
		let mut b = busy.borrow_mut();
		b.refcount -= count;
		if b.refcount == 0 {
			let lru_index = self.lru.add(b.key, size);
			trace!(info "add {:?}", lru_index);
			drop(b);
			*refcount = RefCount::NoRef { lru_index };
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
	) -> Result<(Key, Idx), WakerQueueTicket<()>> {
		let mut do_evict = self.lru.has_excess();

		// If there is not enough room to fetch even one entry, evict even if the LRU disagrees.
		do_evict |= !self.total.has_room_for_entry(max_record_size);

		if let Some(v) = do_evict.then(|| self.lru.last()).flatten() {
			Ok(v)
		} else {
			Err(self.soft_wakers.push(waker.clone(), ()))
		}
	}

	/// Get a mutable reference to a key held by a node.
	#[must_use]
	pub fn get_key_mut(&mut self, handle: Idx) -> Option<&mut Key> {
		self.lru.get_mut(handle)
	}

	/// Stop tracking *soft* usage of data.
	#[must_use]
	fn soft_remove(&mut self, refcount: &RefCount, size: usize) -> Rc<RefCell<Busy>> {
		trace!("soft_remove {:?} {}", refcount, size);
		match refcount {
			RefCount::Ref { busy } => busy.clone(),
			RefCount::NoRef { lru_index } => {
				let key = self.lru.remove(*lru_index, size);
				Busy::new(key)
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
	pub fn touch(&mut self, refcount: &RefCount) {
		if let RefCount::NoRef { lru_index } = refcount {
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

/// Object-specific impl
impl MemoryTracker {
	/// Mark fetch of object as finished.
	///
	/// This wakes all tasks waiting on the busy entry.
	#[must_use]
	pub fn finish_fetch_object(&mut self, busy: Rc<RefCell<Busy>>) -> RefCount {
		self.finish_fetch(busy, OBJECT_COST)
	}

	/// Increase reference count to object by `count`.
	pub(crate) fn incr_object_refcount(&mut self, refcount: &mut RefCount, count: usize) {
		self.incr_refcount(refcount, OBJECT_COST, count)
	}

	/// Decrease reference count to object by `count`.
	///
	/// # Panics
	///
	/// If there are no live references.
	pub(crate) fn decr_object_refcount(&mut self, refcount: &mut RefCount, count: usize) {
		self.decr_refcount(refcount, OBJECT_COST, count)
	}

	/// Stop tracking *soft* usage of an object.
	#[must_use]
	pub(crate) fn soft_remove_object(&mut self, refcount: &RefCount) -> Rc<RefCell<Busy>> {
		self.soft_remove(refcount, OBJECT_COST)
	}

	/// Stop tracking *hard* usage of an object.
	///
	/// Must be used in combination with [`Self::soft_remove_object`].
	pub(crate) fn hard_remove_object(&mut self) {
		self.hard_remove(OBJECT_COST)
	}
}

/// Entry-specific impl
impl MemoryTracker {
	/// Mark fetch of entry as finished.
	///
	/// This wakes all tasks waiting on the busy entry.
	#[must_use]
	pub fn finish_fetch_entry(&mut self, busy: Rc<RefCell<Busy>>, length: usize) -> RefCount {
		self.finish_fetch(busy, ENTRY_COST + length)
	}

	/// Increase reference count to entry by `count`.
	///
	/// # Panics
	///
	/// If there are no live references.
	pub(crate) fn decr_entry_refcount(
		&mut self,
		refcount: &mut RefCount,
		length: usize,
		count: usize,
	) {
		self.decr_refcount(refcount, ENTRY_COST + length, count)
	}

	/// Stop tracking *soft* usage of an entry.
	#[must_use]
	pub(crate) fn soft_remove_entry(
		&mut self,
		refcount: &RefCount,
		length: usize,
	) -> Rc<RefCell<Busy>> {
		self.soft_remove(refcount, ENTRY_COST + length)
	}

	/// Stop tracking *hard* usage of an entry.
	///
	/// Must be used in combination with [`Self::soft_remove_entry`].
	pub(crate) fn hard_remove_entry(&mut self, length: usize) {
		self.hard_remove(ENTRY_COST + length)
	}
}

impl<D: Dev, R: Resource> super::Cache<D, R> {
	/// Reserve the given amount of memory.
	///
	/// If it is not immediately available, this function will block.
	async fn memory_reserve(&self, size: usize) {
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

	/// Reserve the given amount of memory for an object.
	///
	/// If it is not immediately available, this function will block.
	pub(super) async fn memory_reserve_object(&self) {
		self.memory_reserve(OBJECT_COST).await
	}

	/// Wait until the total memory usage drops below the limit.
	///
	/// If it is not immediately available, this function will block.
	pub(super) async fn memory_compensate_forced_grow(&self) {
		self.memory_reserve(0).await
	}

	/// Check if cache size matches real usage
	#[cfg(test)]
	#[track_caller]
	pub(crate) fn verify_cache_usage(&self) {
		use super::{Buf, Present, Slot};
		let data = &mut *self.data.borrow_mut();
		let real_usage = data.objects.values_mut().fold(0, |x, s| {
			let mut y = 0;
			if let Slot::Present(slot) = s {
				if matches!(slot.refcount, RefCount::NoRef { .. }) {
					y += OBJECT_COST;
				}
				y += slot
					.data()
					.iter()
					.flat_map(|m| m.slots.values())
					.flat_map(|s| match s {
						Slot::Present(Present { data, refcount: RefCount::NoRef { .. } }) => {
							Some(data)
						}
						_ => None,
					})
					.fold(0, |x, v| x + v.len() + ENTRY_COST);
			}
			x + y
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
