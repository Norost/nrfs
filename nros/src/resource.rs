use core::future::Future;

/// Trait for structures providing various resources.
///
/// Current resources include:
///
/// * Memory allocation.
/// * Threads for parallel processing.
pub trait Resource {
	/// Type representing a region of memory.
	type Buf<'a>: Buf
	where
		Self: 'a;

	/// Type representing a running task.
	type Task<'a>: Future<Output = ()>
	where
		Self: 'a;

	/// Create an empty memory buffer.
	fn alloc(&self) -> Self::Buf<'_>;

	/// Run the given closure.
	fn run(&self, f: Box<dyn FnOnce() + Send + 'static>) -> Self::Task<'_>;
}

/// Type representing a region of memory.
pub trait Buf {
	/// Get a mutable reference to the underlying data.
	fn get_mut(&mut self) -> &mut [u8];
}

#[cfg(not(no_std))]
mod std {
	use {
		super::*,
		core::{
			pin::Pin,
			task::{Context, Poll},
		},
	};

	/// [`Resource`] for use with `std` applications.
	#[derive(Debug)]
	pub struct StdResource {}

	impl StdResource {
		pub fn new() -> Self {
			Self {}
		}
	}

	impl Resource for StdResource {
		type Buf<'a> = Vec<u8>
		where
			Self: 'a;

		type Task<'a> = RunTask
		where
			Self: 'a;

		fn alloc(&self) -> Self::Buf<'_> {
			Vec::new()
		}

		fn run(&self, f: Box<dyn FnOnce() + Send + 'static>) -> Self::Task<'_> {
			let (send, recv) = futures_channel::oneshot::channel::<()>();
			rayon::spawn(move || {
				f();
				send.send(()).unwrap()
			});
			RunTask { recv }
		}
	}

	impl Buf for Vec<u8> {
		fn get_mut(&mut self) -> &mut [u8] {
			self
		}
	}

	/// Computationally expensive task running in parallel.
	pub struct RunTask {
		recv: futures_channel::oneshot::Receiver<()>,
	}

	impl Future for RunTask {
		type Output = ();

		fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
			Pin::new(&mut self.recv).poll(cx).map(|r| r.unwrap())
		}
	}
}

#[cfg(not(no_std))]
pub use self::std::*;
