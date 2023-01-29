use core::future::Future;

/// Trait for structures providing various resources.
///
/// Current resources include:
///
/// * Memory allocation.
/// * Threads for parallel processing.
pub trait Resource {
	/// Type representing a region of memory.
	type Buf: Buf + 'static;

	/// Type representing a running task.
	type Task<'a, R>: Future<Output = R>
	where
		Self: 'a,
		R: 'static;

	/// Create an empty memory buffer.
	fn alloc(&self) -> Self::Buf;

	/// Run the given closure.
	fn run<F, R>(&self, f: F) -> Self::Task<'_, R>
	where
		F: (FnOnce() -> R) + Send + 'static,
		R: Send + 'static;

	/// Get data from a cryptographically secure random source.
	fn crng_fill(&self, buf: &mut [u8]);
}

/// Type representing a region of memory.
pub trait Buf: Send {
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
		rand::RngCore,
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

		type Task<'a, R> = RunTask<R>
		where
			Self: 'a,
			R: 'static;

		fn alloc(&self) -> Self::Buf {
			Vec::new()
		}

		fn run<F, R>(&self, f: F) -> Self::Task<'_, R>
		where
			F: (FnOnce() -> R) + Send + 'static,
			R: Send + 'static,
		{
			#[cfg(feature = "parallel")]
			{
				let (send, recv) = futures_channel::oneshot::channel::<R>();
				rayon::spawn(move || {
					send.send(f())
						.unwrap_or_else(|_| panic!("channel sent no data"));
				});
				RunTask { recv }
			}
			#[cfg(not(feature = "parallel"))]
			{
				RunTask { result: core::future::ready(f()) }
			}
		}

		fn crng_fill(&self, buf: &mut [u8]) {
			if cfg!(any(test, fuzzing)) {
				// For determinism when testing.
				buf.fill(0);
			} else {
				rand::thread_rng().fill_bytes(buf);
			}
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
	pub struct RunTask<R> {
		#[cfg(feature = "parallel")]
		recv: futures_channel::oneshot::Receiver<R>,
		#[cfg(not(feature = "parallel"))]
		result: core::future::Ready<R>,
	}

	impl<R> Future for RunTask<R> {
		type Output = R;

		fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
			#[cfg(feature = "parallel")]
			{
				Pin::new(&mut self.recv).poll(cx).map(|r| r.unwrap())
			}
			#[cfg(not(feature = "parallel"))]
			{
				Pin::new(&mut self.result).poll(cx)
			}
		}
	}
}

#[cfg(not(no_std))]
pub use self::std::*;
