mod busy;
mod tracker;

pub(super) use busy::BusyState;
pub use tracker::{Idx, IDX_NONE};

use {
	super::IdKey,
	crate::{waker_queue, MaxRecordSize},
};

/// Memory management.
#[derive(Debug)]
pub struct Mem {
	/// Entries currently being fetched or actively in use by a task.
	pub busy: busy::BusyMap,
	/// Memory usage tracker.
	pub tracker: tracker::MemoryTracker,
}

impl Mem {
	/// Move an entry from the Max to the Empty state.
	pub fn max_to_empty(&mut self, max_rec_size: MaxRecordSize) {
		trace!("mem_max_to_empty");
		self.tracker.hard_del(mem_max(max_rec_size));
	}

	/// Move an entry from the Exact to the Empty state.
	pub fn exact_to_empty(&mut self, max_rec_size: MaxRecordSize, idx: Idx, size: usize) -> IdKey {
		trace!("mem_exact_to_empty {}", size);
		self.tracker.hard_del(size);
		self.tracker.soft_del(idx, size)
	}

	/// Move an entry from the Max to Exact state.
	pub fn max_to_exact(&mut self, max_rec_size: MaxRecordSize, key: IdKey, size: usize) -> Idx {
		trace!("mem_max_to_exact {}", size);
		self.tracker.hard_del(mem_max(max_rec_size) - size);
		self.tracker.soft_add(key, size)
	}
}

impl<D: crate::Dev, R: crate::Resource> super::Cache<D, R> {
	async fn mem_hard_add(&self, amount: usize) {
		waker_queue::poll(
			move |cx| match self.mem().tracker.hard_add(amount, cx.waker()) {
				Some(t) => Err(t),
				None => Ok(()),
			},
		)
		.await
	}

	/// Reserve memory for transitioning an entry from Empty to the Max state.
	///
	/// Since memory may not be immediately available this function may block.
	pub(super) async fn mem_empty_to_max(&self) {
		trace!("mem_empty_to_max");
		self.mem_hard_add(mem_max(self.max_rec_size())).await;
	}

	/// Move an entry from the Exact to Max state.
	///
	/// Since memory may not be immediately available this function may block.
	pub(super) async fn mem_idle_to_busy(&self, idx: Idx, size: usize) {
		trace!("mem_idle_to_busy {}", size);
		self.mem().tracker.soft_del(idx, size);
		self.mem_hard_add(mem_max(self.max_rec_size()) - size).await;
	}

	/// Check if cache size matches real usage
	#[track_caller]
	pub(super) fn verify_cache_usage(&self) {
		if !(cfg!(test) || cfg!(fuzzing)) {
			return;
		}
		let data = &*self.data();
		let real_usage = data.objects.iter().fold(0, |x, (&id, obj)| {
			x + obj
				.records
				.iter()
				.filter(|&(&key, _)| !data.mem.busy.has(IdKey { id, key }))
				.fold(0, |x, (_, d)| x + v.data.len() + tracker::ENTRY_COST)
		});
		assert_eq!(
			real_usage,
			data.mem.tracker.lru.size(),
			"cache size mismatch"
		);
		assert!(
			data.mem.tracker.lru.size() <= data.mem.tracker.total.usage(),
			"lru size larger than total usage"
		);
	}
}

fn mem_max(max_rec_size: MaxRecordSize) -> usize {
	tracker::ENTRY_COST + (1 << max_rec_size.to_raw())
}
