use {
	super::super::IdKey,
	crate::{
		util::BTreeMapExt,
		waker_queue::{WakerQueue, WakerQueueTicket},
	},
	alloc::collections::BTreeMap,
	core::task::Waker,
};

/// Map of reference counters.
#[derive(Debug, Default)]
pub(super) struct BusyMap {
	busy: BTreeMap<IdKey, Busy>,
}

pub enum BusyState {
	/// The entry just transitioned to the busy (wait mem) state.
	New,
	/// The entry is waiting for a memory reservation from another task.
	WaitMem,
	/// The entry is ready for use.
	Ready,
}

#[derive(Debug)]
struct Busy {
	/// Current state.
	state: State,
	/// The amount of tasks referencing this data.
	///
	/// *Excludes* the busy task.
	refcount: usize,
}

#[derive(Debug)]
enum State {
	WaitMem { wakers: WakerQueue<()> },
	Ready,
}

impl BusyMap {
	/// Reference an entry.
	pub fn incr(&mut self, key: IdKey) -> BusyState {
		let busy = self.busy.entry(key).or_insert_with(|| Busy {
			state: State::WaitMem { wakers: Default::default() },
			refcount: 0,
		});
		busy.refcount += 1;
		match (&busy.state, busy.refcount) {
			(State::WaitMem { .. }, 1) => BusyState::New,
			(State::WaitMem { .. }, _) => BusyState::WaitMem,
			(State::Ready, _) => BusyState::Ready,
		}
	}

	/// Dereference an entry.
	///
	/// If it returns `true`, no other entries are referencing this entry
	/// and associated memory reservations must be released.
	///
	/// # Panics
	///
	/// There is no busy entry.
	pub fn decr(&mut self, key: IdKey) -> bool {
		let mut busy = self.busy.occupied(key).expect("not busy");
		busy.get_mut().refcount -= 1;
		let no_refs = busy.get().refcount == 0;
		if no_refs {
			busy.remove();
		}
		no_refs
	}

	/// Transition an entry to the `Busy (ready)` state.
	///
	/// # Panics
	///
	/// There is no busy entry.
	///
	/// The entry is already in the ready state.
	pub fn mark_ready(&mut self, key: IdKey) {
		let mut busy = self.busy.occupied(key).expect("not busy");
		let State::WaitMem { wakers } = &mut busy.get_mut().state
			else { panic!("not in wait mem state") };
		wakers.wake_all();
		busy.get_mut().state = State::Ready;
	}

	/// Poll an entry.
	///
	/// If it is in the `Busy (wait mem)` state, a ticket is returned.
	///
	/// # Panics
	///
	/// There is no busy entry.
	pub fn poll(&mut self, key: IdKey, waker: &Waker) -> Option<WakerQueueTicket<()>> {
		let mut busy = self.busy.occupied(key).expect("not busy");
		match &mut busy.get_mut().state {
			State::WaitMem { wakers } => Some(wakers.push(waker.clone(), ())),
			State::Ready => None,
		}
	}

	pub fn has(&self, key: IdKey) -> bool {
		self.busy.contains_key(&key)
	}
}
