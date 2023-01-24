use {
	core::{
		cell::RefCell,
		fmt,
		future::{self, Future},
		pin::Pin,
		task::{Poll, Waker},
	},
	futures_util::{stream::FuturesUnordered, FutureExt, Stream},
};

type Task<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Background task runner.
///
/// This is used for tasks that have no immediate effect on user applications.
pub struct Background<'a, T> {
	inner: RefCell<Inner<'a, T>>,
	// Tell the user very loudly to use Background::drop
	_anti_drop: Guard,
}

struct Inner<'a, T> {
	/// Active background tasks.
	tasks: FuturesUnordered<Task<'a, T>>,
	/// Waker for completing background tasks.
	waker: Option<Waker>,
}

impl<'a, T: 'a> Background<'a, T> {
	/// Add a background task.
	pub(crate) fn run_background(&self, task: Task<'a, T>) {
		trace!("run_background");

		#[cfg(feature = "trace")]
		let task = {
			let mut task = task;
			let id = crate::trace::gen_taskid();
			Box::pin(core::future::poll_fn(move |cx| {
				let _trace = crate::trace::TraceTask::new(id);
				Pin::new(&mut task).poll(cx)
			}))
		};

		let mut bg = self.inner.borrow_mut();
		bg.tasks.push(task);
		bg.waker.take().map(|w| w.wake());
	}
}

impl<'a, E> Background<'a, Result<(), E>> {
	/// Try to complete all background tasks.
	///
	/// Returns as soon as an error is caught.
	pub(crate) async fn try_run_all(&self) -> Result<(), E> {
		trace!("Background::try_run_all");
		future::poll_fn(|cx| {
			let mut bg = self.inner.borrow_mut();
			match Pin::new(&mut bg.tasks).poll_next(cx) {
				Poll::Ready(None) => Poll::Ready(Ok(())),
				Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
				Poll::Ready(Some(Ok(()))) => {
					cx.waker().wake_by_ref();
					Poll::Pending
				}
				Poll::Pending => {
					let _ = bg.waker.insert(cx.waker().clone());
					Poll::Pending
				}
			}
		})
		.await
	}

	pub async fn drop(self) -> Result<(), E> {
		trace!("Background::drop");

		let res = self.try_run_all().await;

		let Self { inner, _anti_drop } = self;
		core::mem::forget(_anti_drop);

		if res.is_err() {
			inner.borrow_mut().tasks.clear();
		}
		debug_assert!(inner.borrow_mut().tasks.is_empty());
		res
	}

	/// Poll & complete background tasks.
	///
	/// This future never finishes unless an error occurs.
	pub async fn process_background(&self) -> Result<!, E> {
		trace!("Background::process_background");
		future::poll_fn(|cx| {
			let mut bg = self.inner.borrow_mut();
			match Pin::new(&mut bg.tasks).poll_next(cx) {
				Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
				Poll::Ready(Some(Ok(()))) => {
					cx.waker().wake_by_ref();
					Poll::Pending
				}
				Poll::Ready(None) | Poll::Pending => {
					let _ = bg.waker.insert(cx.waker().clone());
					Poll::Pending
				}
			}
		})
		.await
	}

	/// Run the given task while polling background tasks.
	pub async fn run<F, R, EE>(&self, f: F) -> Result<R, EE>
	where
		F: Future<Output = Result<R, EE>>,
		EE: From<E>,
	{
		let mut f = core::pin::pin!(f.fuse());
		let mut bg = core::pin::pin!(self.process_background().fuse());
		futures_util::select_biased! {
			r = bg => r.map(|r| r).map_err(EE::from),
			r = f => r,
		}
	}
}

impl<T> Default for Background<'_, T> {
	fn default() -> Self {
		Self {
			inner: Inner { tasks: Default::default(), waker: Default::default() }.into(),
			_anti_drop: Guard,
		}
	}
}

impl<T> fmt::Debug for Background<'_, T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let bg = self.inner.borrow_mut();
		f.debug_struct(stringify!(Background))
			.field("tasks", &format_args!("[ ... ] (len: {})", bg.tasks.len()))
			.field("waker", &bg.waker)
			.finish()
	}
}

struct Guard;

#[cfg(debug_assertions)]
impl Drop for Guard {
	fn drop(&mut self) {
		eprintln!("use Background::drop");
	}
}
