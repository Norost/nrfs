use core::future::Future;

/// Trait for structures providing various resources.
///
/// Current resources include:
///
/// * Memory allocation.
/// * Threads for parallel processing.
pub trait Resource {
	/// Type representing a region of memory.
	type Buf: Buf;

	/// Type representing a running task.
	type Task<'a>: Future<Output = ()>
	where
		Self: 'a;

	/// Create an empty memory buffer.
	fn alloc(&self) -> Self::Buf;

	/// Run the given closure.
	fn run(&self, f: Box<dyn FnOnce() + Send + 'static>) -> Self::Task<'_>;
}

/// Type representing a region of memory.
pub trait Buf: Clone {
	/// Get an immutable reference to the underlying data.
	fn get(&self) -> &[u8];

	/// Get a mutable reference to the underlying data.
	fn get_mut(&mut self) -> &mut [u8];

	/// The length of the buffer.
	fn len(&self) -> usize {
		self.get().len()
	}

	/// Resize the buffer.
	fn resize(&mut self, new_len: usize, fill: u8);

	/// Shrink the capacity of the buffer.
	fn shrink(&mut self);

	/// Get the capacity of the buffer.
	fn capacity(&self) -> usize;

	fn extend_from_slice(&mut self, slice: &[u8]) {
		let len = self.len();
		self.resize(len + slice.len(), 0);
		self.get_mut()[len..].copy_from_slice(slice);
	}
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
		type Buf = Vec<u8>;

		type Task<'a> = RunTask
		where
			Self: 'a;

		fn alloc(&self) -> Self::Buf {
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
		fn get(&self) -> &[u8] {
			self
		}

		fn get_mut(&mut self) -> &mut [u8] {
			self
		}

		fn resize(&mut self, new_len: usize, fill: u8) {
			Vec::resize(self, new_len, fill)
		}

		fn shrink(&mut self) {
			Vec::shrink_to_fit(self)
		}

		fn capacity(&self) -> usize {
			Vec::capacity(self)
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
