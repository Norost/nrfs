use {
	core::{
		cell::RefCell,
		fmt,
		future::{self, Future},
		pin::Pin,
		task::{Poll, Waker},
	},
	futures_util::{stream::FuturesUnordered, FutureExt, Stream, TryStreamExt},
};

/// Background task runner.
///
/// This is used for tasks that have no immediate effect on user applications.
pub struct Background<Fut> {
	inner: RefCell<Inner<Fut>>,
	// Tell the user very loudly to use Background::drop
	_anti_drop: Guard,
}

struct Inner<Fut> {
	/// Active background tasks.
	tasks: FuturesUnordered<Fut>,
	/// Waker for completing background tasks.
	waker: Option<Waker>,
}

impl<Fut> Background<Fut> {
	/// Add a background task.
	pub(crate) fn run_background(&self, task: Fut) {
		trace!("run_background");
		let mut bg = self.inner.borrow_mut();
		bg.tasks.push(task);
		bg.waker.take().map(|w| w.wake());
	}
}

impl<Fut: Future<Output = Result<(), E>>, E> Background<Fut> {
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
		let Self { inner, _anti_drop } = self;
		core::mem::forget(_anti_drop);
		let mut bg = inner.into_inner();
		let res = Pin::new(&mut bg.tasks)
			.try_for_each(|_| future::ready(Ok(())))
			.await;
		if res.is_err() {
			bg.tasks.clear();
		}
		debug_assert!(bg.tasks.is_empty());
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

impl<Fut> Default for Background<Fut> {
	fn default() -> Self {
		Self {
			inner: Inner { tasks: Default::default(), waker: Default::default() }.into(),
			_anti_drop: Guard,
		}
	}
}

impl<Fut> fmt::Debug for Background<Fut> {
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
