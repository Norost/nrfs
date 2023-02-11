use core::{
	cell::RefCell,
	fmt::Arguments,
	future::Future,
	mem,
	pin::Pin,
	task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

#[derive(Default)]
struct Tracker {
	task_depth: alloc::collections::BTreeMap<u64, usize>,
	task_stack: Vec<u64>,
	id_counter: u64,
}

thread_local! {
	static TRACKER: RefCell<Tracker> = Default::default();
}

fn with<R>(f: impl FnOnce(&mut Tracker) -> R) -> R {
	TRACKER.with(|t| f(&mut t.borrow_mut()))
}

#[no_coverage]
pub fn print_debug(prefix: &str, args: &Arguments<'_>) {
	with(|t| {
		let id = *t.task_stack.last().unwrap_or(&0);
		let depth = *t.task_depth.get(&id).unwrap_or(&0);
		eprintln!(
			"[nros:<{}>]{:>pad$} {}{}",
			id,
			"",
			prefix,
			args,
			pad = depth * 2
		);
	});
}

pub struct Trace(u64);

impl Trace {
	#[no_coverage]
	pub fn new() -> Self {
		with(|t| {
			let id = *t.task_stack.last().unwrap_or(&0);
			*t.task_depth.entry(id).or_default() += 1;
			Self(id)
		})
	}
}

impl Drop for Trace {
	#[no_coverage]
	fn drop(&mut self) {
		with(|t| {
			let depth = t.task_depth.get_mut(&self.0).unwrap();
			*depth -= 1;
			if *depth == 0 {
				t.task_depth.remove(&self.0).unwrap();
			}
		});
	}
}

#[no_coverage]
pub fn gen_taskid() -> u64 {
	with(|t| {
		t.id_counter += 1;
		t.id_counter
	})
}

pub struct TraceTask;

impl TraceTask {
	#[no_coverage]
	pub fn new(id: u64) -> Self {
		with(|t| t.task_stack.push(id));
		Self
	}
}

impl Drop for TraceTask {
	#[no_coverage]
	fn drop(&mut self) {
		with(|t| t.task_stack.pop());
	}
}

pub struct TracedTask<F> {
	id: u64,
	task: F,
	finished: bool,
}

impl<F> TracedTask<F> {
	pub fn new(task: F) -> Self {
		let id = crate::trace::gen_taskid();
		trace!(info "id {}", id);
		Self { id, task, finished: false }
	}
}

impl<F: Future> Future for TracedTask<F> {
	type Output = F::Output;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		assert!(!self.finished, "already finished");

		let _trace = TraceTask::new(self.id);
		let waker = traced_waker(self.id, cx.waker().clone());
		let cx = &mut Context::from_waker(&waker);

		// SAFETY: self is pinned too
		let mut task = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.task) };
		if let Poll::Ready(r) = task.poll(cx) {
			trace!(info "done");
			// SAFETY: finished doesn't apply to task
			unsafe { self.get_unchecked_mut().finished = true };
			return Poll::Ready(r);
		}
		Poll::Pending
	}
}

impl<F> Drop for TracedTask<F> {
	fn drop(&mut self) {
		let _trace = TraceTask::new(self.id);
		if !self.finished {
			trace!(info "not finished")
		}
	}
}

/// Waker that logs the ID of the task being woken up.
#[derive(Clone)]
struct TracedWakerData {
	id: u64,
	waker: Waker,
}

fn traced_waker(id: u64, waker: Waker) -> Waker {
	let w = Box::new(TracedWakerData { id, waker });
	let w = RawWaker::new(Box::into_raw(w) as _, &WAKER_VTABLE);
	unsafe { Waker::from_raw(w) }
}

static WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
	// clone
	|ptr| unsafe {
		let data = Box::new((*(ptr as *mut TracedWakerData)).clone());
		RawWaker::new(Box::into_raw(data) as _, &WAKER_VTABLE)
	},
	// wake
	|ptr| unsafe {
		let data = Box::from_raw(ptr as *mut TracedWakerData);
		let _trace = TraceTask::new(data.id);
		trace!("waking up");
		data.waker.wake();
	},
	// wake_by_ref
	|ptr| unsafe {
		let data = &*(ptr as *mut TracedWakerData);
		let _trace = TraceTask::new(data.id);
		trace!("waking up");
		data.waker.wake_by_ref();
	},
	// drop
	|ptr| unsafe {
		drop(Box::from_raw(ptr as *mut TracedWakerData));
	},
);
