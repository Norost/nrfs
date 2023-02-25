mod lru;

pub use lru::{Idx, IDX_NONE};

use {
	super::IdKey,
	crate::waker_queue::{self, WakerQueue, WakerQueueTicket},
	core::task::Waker,
};

/// Memory management.
#[derive(Debug)]
pub(super) struct Mem {
	/// Amount of entries beyond which tasks will block.
	hard_limit: usize,
	/// Current amount of reserved & used slots.
	hard_count: usize,
	/// Tasks waiting for an entry slot to become free.
	hard_wakers: WakerQueue<()>,
	/// Amount of entries beyond which entries will be evicted.
	soft_limit: usize,
	/// LRU tracker for unreferenced entries.
	lru: lru::LruList<IdKey>,
	/// Tasks to wake if there are more entries to evict.
	soft_wakers: WakerQueue<()>,
}

impl Mem {
	pub fn new(soft_limit: usize, hard_limit: usize) -> Self {
		Self {
			hard_limit,
			hard_count: 0,
			hard_wakers: Default::default(),
			soft_limit,
			lru: Default::default(),
			soft_wakers: Default::default(),
		}
	}

	#[must_use = "LRU idx"]
	pub fn soft_add(&mut self, key: IdKey) -> Idx {
		trace!("soft_add {:?}", key);
		self.soft_wakers.wake_all();
		self.lru.insert(key)
	}

	pub fn soft_del(&mut self, idx: Idx) -> IdKey {
		trace!("soft_del {:?}", idx);
		self.lru.remove(idx)
	}

	pub fn hard_del(&mut self) {
		trace!("hard_del");
		self.hard_count -= 1;
		self.hard_wakers.wake_next();
	}

	pub fn evict_next(&mut self, waker: &Waker) -> Result<IdKey, WakerQueueTicket<()>> {
		(self.lru.len() > self.soft_limit)
			.then(|| self.lru.last())
			.flatten()
			.map(|(_, k)| *k)
			.ok_or_else(|| self.soft_wakers.push(waker.clone(), ()))
	}

	pub fn hard_count(&self) -> usize {
		self.hard_count
	}

	pub fn soft_count(&self) -> usize {
		self.lru.len()
	}

	pub fn set_soft_limit(&mut self, value: usize) {
		self.soft_limit = value;
		self.soft_wakers.wake_next();
	}
}

impl<D: crate::Dev, R: crate::Resource> super::Cache<D, R> {
	pub async fn mem_hard_add(&self) {
		trace!("hard_add");
		waker_queue::poll(|cx| {
			let mut mem = self.mem();
			if mem.hard_count < mem.hard_limit {
				mem.hard_count += 1;
				return Ok(());
			}
			Err(mem.hard_wakers.push(cx.waker().clone(), ()))
		})
		.await
	}
}
