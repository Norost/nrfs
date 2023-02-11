use {
	core::{
		cell::RefCell,
		fmt,
		future::{self, Future},
		pin::Pin,
		task::{Poll, Waker},
	},
	futures_util::{stream::FuturesUnordered, Stream},
};

type Task<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Background task runner.
///
/// This structure runs various tasks submitted by [`Nros`] itself.
///
/// It must be continuously polled to ensure user-submitted tasks can always progress.
pub struct Background<'a, T> {
	inner: RefCell<Inner<'a, T>>,
}

struct Inner<'a, T> {
	/// Active background tasks.
	tasks: FuturesUnordered<Task<'a, T>>,
	/// Waker for completing background tasks.
	waker: Option<Waker>,
}

impl<'a, T: 'a> Background<'a, T> {
	/// Add a background task.
	pub(crate) fn add(&self, task: Task<'a, T>) {
		trace!("Background::add");

		#[cfg(feature = "trace")]
		let task = Box::pin(crate::trace::TracedTask::new(task));

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
			loop {
				break match Pin::new(&mut bg.tasks).poll_next(cx) {
					Poll::Ready(None) => Poll::Ready(Ok(())),
					Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
					Poll::Ready(Some(Ok(()))) => continue,
					Poll::Pending => {
						let _ = bg.waker.insert(cx.waker().clone());
						Poll::Pending
					}
				};
			}
		})
		.await
	}

	/// Poll & complete background tasks.
	///
	/// This future never finishes unless an error occurs.
	pub async fn process(&self) -> Result<!, E> {
		future::poll_fn(|cx| {
			let mut bg = self.inner.borrow_mut();
			loop {
				break match Pin::new(&mut bg.tasks).poll_next(cx) {
					Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
					Poll::Ready(Some(Ok(()))) => continue,
					Poll::Ready(None) | Poll::Pending => {
						let _ = bg.waker.insert(cx.waker().clone());
						Poll::Pending
					}
				};
			}
		})
		.await
	}
}

impl<T> Default for Background<'_, T> {
	/// Create a new background runner.
	fn default() -> Self {
		Self { inner: Inner { tasks: Default::default(), waker: Default::default() }.into() }
	}
}

impl<T> fmt::Debug for Background<'_, T> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let bg = self.inner.borrow_mut();
		f.debug_struct(stringify!(Background))
			.field("tasks", &format_args!("[ ... ] (len: {})", bg.tasks.len()))
			.field("waker", &bg.waker)
			.finish()
	}
}
