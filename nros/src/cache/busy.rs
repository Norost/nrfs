use {
	super::IdKey,
	crate::{
		util::BTreeMapExt,
		waker_queue::{WakerQueue, WakerQueueTicket},
	},
	alloc::collections::BTreeMap,
	core::task::Waker,
};

#[derive(Default, Debug)]
pub(super) struct BusyMap {
	map: BTreeMap<IdKey, Busy>,
}

#[derive(Default, Debug)]
struct Busy {
	/// Tasks referencing the entry.
	///
	/// A record with a non-zero reference count cannot be evicted.
	refcount: u32,
	/// Tasks waiting for the entry to be present.
	wakers: WakerQueue<()>,
}

impl BusyMap {
	/// Increase reference count.
	///
	/// If the reference count was 0 previously, returns `true`.
	pub fn incr(&mut self, key: IdKey) -> bool {
		trace!("busy_incr {:?}", key);
		let e = self.map.entry(key).or_default();
		e.refcount += 1;
		e.refcount == 1
	}

	/// Insert a waker to be woken when an entry becomes present.
	pub fn wait(&mut self, key: IdKey, waker: Waker) -> WakerQueueTicket<()> {
		trace!("busy_wait {:?}", key);
		self.map
			.get_mut(&key)
			.expect("not busy")
			.wakers
			.push(waker, ())
	}

	/// Wake tasks waiting for the entry to become present.
	pub fn wake(&mut self, key: IdKey) {
		trace!("busy_wake {:?}", key);
		self.map.get_mut(&key).expect("not busy").wakers.wake_all();
	}

	/// Decrease reference count.
	///
	/// If the reference count has become 0, returns `true`.
	pub fn decr(&mut self, key: IdKey) -> bool {
		trace!("busy_decr {:?}", key);
		let mut e = self.map.occupied(key).expect("not busy");
		e.get_mut().refcount -= 1;
		let no_ref = e.get().refcount == 0;
		if no_ref {
			debug_assert!(
				e.get().wakers.get_next().is_none(),
				"wakers in busy entry with no refs"
			);
			e.remove();
		}
		no_ref
	}
}
