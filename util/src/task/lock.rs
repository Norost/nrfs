use {
	super::waker_queue::{self, WakerQueue},
	core::cell::{Cell, UnsafeCell},
};

/// A single lock.
///
/// Each lock can be acquired inclusively or can be exclusive,
/// in which case no other locks may be held.
#[derive(Debug, Default)]
pub struct Lock {
	/// The amount of readers or (sole) writer.
	///
	/// If the high bit is set, a task is attempting to acquire an exclusive lock.
	count: Cell<isize>,
	/// Tasks waiting for the lock to be released.
	queue: UnsafeCell<WakerQueue<()>>,
}

impl Lock {
	/// Acquire an inclusive lock.
	pub async fn lock_inclusive(&self) -> LockInclusiveGuard<'_> {
		waker_queue::poll(|cx| {
			// SAFETY:
			// * The mutable reference will be dropped at the end of this function
			// * We won't acquire another mutable reference.
			let queue = unsafe { &mut *self.queue.get() };
			// Give priority to exclusive locks.
			if self.count.get() & isize::MIN != 0 {
				Err(queue.push(cx.waker().clone(), ()))
			} else {
				// Mark as having acquired the lock.
				self.count.update(|x| x + 1);
				Ok(())
			}
		})
		.await;
		LockInclusiveGuard { lock: self }
	}

	/// Acquire an exclusive lock.
	pub async fn lock_exclusive(&self) -> LockExclusiveGuard<'_> {
		waker_queue::poll(|cx| {
			// SAFETY:
			// * The mutable reference will be dropped at the end of this function
			// * We won't acquire another mutable reference.
			let queue = unsafe { &mut *self.queue.get() };
			if self.count.get() & isize::MAX != 0 {
				// Mark as attempting to acquire the lock.
				self.count.update(|x| x | isize::MIN);
				Err(queue.push(cx.waker().clone(), ()))
			} else {
				// Mark as having acquired the lock.
				self.count.set(isize::MIN | 1);
				Ok(())
			}
		})
		.await;
		LockExclusiveGuard { lock: self }
	}
}

pub struct LockInclusiveGuard<'a> {
	lock: &'a Lock,
}

impl Drop for LockInclusiveGuard<'_> {
	fn drop(&mut self) {
		// SAFETY:
		// * The mutable reference will be dropped at the end of this function
		// * We won't acquire another mutable reference.
		let queue = unsafe { &mut *self.lock.queue.get() };

		debug_assert!(
			self.lock.count.get() & isize::MAX > 0,
			"lock not inclusively acquired"
		);

		let count = self.lock.count.update(|x| x - 1);
		if count & isize::MAX == 0 {
			// Wake the next task, which should be a task that is waiting for an exclusive lock.
			queue.wake_next();
		}
	}
}

pub struct LockExclusiveGuard<'a> {
	lock: &'a Lock,
}

impl Drop for LockExclusiveGuard<'_> {
	fn drop(&mut self) {
		// SAFETY:
		// * The mutable reference will be dropped at the end of this function
		// * We won't acquire another mutable reference.
		let queue = unsafe { &mut *self.lock.queue.get() };
		queue.wake_all();

		debug_assert!(
			self.lock.count.get() & isize::MIN != 0,
			"lock not exclusively acquired"
		);

		self.lock.count.update(|x| (x - 1) & isize::MAX);
	}
}

#[cfg(test)]
mod test {
	use {
		super::*,
		core::{
			future::Future,
			pin::{pin, Pin},
			task::{Context, Poll},
		},
	};

	fn poll<F>(f: Pin<&mut F>) -> Poll<F::Output>
	where
		F: Future,
	{
		let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
		f.poll(&mut cx)
	}

	#[test]
	fn lock_inclusive() {
		let s = Lock::default();
		let a = poll(pin!(s.lock_inclusive()));
		assert!(a.is_ready());
		let b = poll(pin!(s.lock_inclusive()));
		assert!(b.is_ready());
	}

	#[test]
	fn lock_exclusive() {
		let s = Lock::default();
		let a = poll(pin!(s.lock_exclusive()));
		assert!(a.is_ready());
		let b = poll(pin!(s.lock_exclusive()));
		assert!(b.is_pending());
	}

	#[test]
	fn lock_inclusive_exclusive() {
		let s = Lock::default();
		let a = poll(pin!(s.lock_inclusive()));
		assert!(a.is_ready());
		let b = poll(pin!(s.lock_exclusive()));
		assert!(b.is_pending());
	}

	#[test]
	fn lock_exclusive_inclusive() {
		let s = Lock::default();
		let a = poll(pin!(s.lock_exclusive()));
		assert!(a.is_ready());
		let b = poll(pin!(s.lock_inclusive()));
		assert!(b.is_pending());
	}

	#[test]
	fn lock_ex_inc_release_ex() {
		let s = Lock::default();
		let a = poll(pin!(s.lock_exclusive()));
		assert!(a.is_ready());
		let mut b = pin!(s.lock_inclusive());
		assert!(poll(b.as_mut()).is_pending());
		drop(a);
		assert!(poll(b.as_mut()).is_ready());
	}

	#[test]
	fn lock_inc_ex_release_inc() {
		let s = Lock::default();
		let a = poll(pin!(s.lock_inclusive()));
		assert!(a.is_ready());
		let mut b = pin!(s.lock_exclusive());
		assert!(poll(b.as_mut()).is_pending());
		drop(a);
		assert!(poll(b.as_mut()).is_ready());
	}

	#[test]
	fn lock_inc_inc_ex_release_inc() {
		let s = Lock::default();

		let a = poll(pin!(s.lock_inclusive()));
		assert!(a.is_ready());
		let b = poll(pin!(s.lock_inclusive()));
		assert!(b.is_ready());

		let mut c = pin!(s.lock_exclusive());
		assert!(poll(c.as_mut()).is_pending());
		drop(a);
		assert!(poll(c.as_mut()).is_pending());
		drop(b);
		assert!(poll(c.as_mut()).is_ready());
	}
}
