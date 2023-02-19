use {
	super::ENTRY_COST,
	crate::{MaxRecordSize, WakerQueue, WakerQueueTicket},
	core::task::Waker,
};

/// Structure keeping track of total cache memory usage.
///
/// This serves as a hard limit to prevent excessive peaks.
#[derive(Debug)]
pub(super) struct TotalMemoryUsage {
	/// Total amount of memory used by entries in cache.
	///
	/// This includes inflight / busy data.
	usage: usize,
	/// Maximum amount of total memory that may be allocated.
	///
	/// This is a hard limit. If it is about to be exceeded by [`Tree::fetch`] or
	/// [`EntryRef::modify`], the current task yields until usage is sufficiently below the limit.
	limit: usize,
	/// Tasks waiting for the total memory usage drops below an acceptable limit.
	///
	/// The first element indicates the amount of memory the task will contribute to the limit.
	/// If this value added to the current usage would cause the limit to be exceeded, the task
	/// is not woken up.
	tasks: WakerQueue<usize>,
}

impl TotalMemoryUsage {
	/// Create new limiter
	///
	/// # Note
	///
	/// `limit` must at least be as large as the sum of the soft limit and the largest amount
	/// of memory that may be requested!
	/// Otherwise some tasks may get stuck waiting for memory that will never be available.
	pub fn new(limit: usize) -> Self {
		Self { usage: 0, limit, tasks: Default::default() }
	}

	/// The current total usage.
	pub fn usage(&self) -> usize {
		self.usage
	}

	/// Add memory to the limit.
	///
	/// If this would exceed the limit, `waker` is added to a queue and an object tracking
	/// whether the waker is still in the queue is returned.
	/// Usage is untouched on failure.
	#[must_use = "check the return value and call `Cache::reserve_memory` if necessary"]
	pub fn add(&mut self, amount: usize, waker: &Waker) -> Option<WakerQueueTicket<usize>> {
		debug_assert!(amount <= self.limit, "amount exceeds limit");

		if self.usage + amount <= self.limit {
			// We can proceed
			self.usage += amount;
			trace!(info "{}/{}", self.usage, self.limit);
			None
		} else {
			Some(self.tasks.push(waker.clone(), amount))
		}
	}

	pub fn force_add(&mut self, amount: usize) {
		debug_assert!(amount <= self.limit, "amount exceeds limit");
		self.usage += amount;
		trace!(info "{}/{}", self.usage, self.limit);
		if self.usage > self.limit {
			trace!(info "over hard limit!");
		}
	}

	/// Remove memory from the usage.
	///
	/// This will wake any waiting tasks if the limit drops below the limit.
	pub fn remove(&mut self, amount: usize) {
		trace!("remove_memory {}", amount);

		// Reduce usage
		self.usage -= amount;
		trace!(info "{}/{}", self.usage, self.limit);

		// Wake tasks
		let mut estimate = self.usage;
		while let Some(node) = self.tasks.get_next() {
			estimate += *node.value();
			if estimate > self.limit {
				// Would exceed limit
				break;
			}
			trace!(info "wake one {}", node.value());
			self.tasks.wake_next();
		}
	}

	/// Check if there is enough room to fetch an entry.
	///
	/// This includes max record size, entry cost & object cost.
	pub fn has_room_for_entry(&self, max_record_size: MaxRecordSize) -> bool {
		let total = ENTRY_COST + (1 << max_record_size.to_raw());
		self.usage + total <= self.limit
	}
}
