use {
	super::waker_queue::{self, WakerQueue},
	crate::BTreeMapExt,
	alloc::collections::BTreeMap,
	core::{cell::UnsafeCell, mem::MaybeUninit},
};

/// A set of locks.
///
/// Each lock can be acquired inclusively or can be exclusive,
/// in which case no other locks may be held.
///
/// This may be more memory-efficient if only a few locks are held for many structures at any time.
#[derive(Debug)]
pub struct LockSet<K> {
	/// Individual locks.
	locks: UnsafeCell<BTreeMap<K, Lock>>,
}

#[derive(Default)]
struct Lock {
	/// The amount of readers or (sole) writer.
	///
	/// If the high bit is set, a task is attempting to acquire an exclusive lock.
	count: isize,
	/// Tasks waiting for the lock to be released.
	queue: WakerQueue<()>,
}

impl<K: Ord + Eq + Clone> LockSet<K> {
	/// Acquire an inclusive lock.
	pub async fn lock_inclusive(&self, key: K) -> LockSetInclusiveGuard<'_, K> {
		let mut immediate = true;
		waker_queue::poll(|cx| {
			// SAFETY:
			// * The mutable reference will be dropped at the end of this function
			// * We won't acquire another mutable reference.
			let locks = unsafe { &mut *self.locks.get() };
			let lock = locks.entry(key.clone()).or_insert_with(Default::default);
			// Give priority to exclusive locks.
			if lock.count & isize::MIN != 0 {
				immediate = false;
				Err(lock.queue.push(cx.waker().clone(), ()))
			} else {
				// Mark as having acquired the lock.
				lock.count += 1;
				Ok(())
			}
		})
		.await;
		LockSetInclusiveGuard { lock_set: self, key: MaybeUninit::new(key), immediate }
	}

	/// Acquire an exclusive lock.
	pub async fn lock_exclusive(&self, key: K) -> LockSetExclusiveGuard<'_, K> {
		let mut immediate = true;
		waker_queue::poll(|cx| {
			// SAFETY:
			// * The mutable reference will be dropped at the end of this function
			// * We won't acquire another mutable reference.
			let locks = unsafe { &mut *self.locks.get() };
			let lock = locks.entry(key.clone()).or_insert_with(Default::default);
			if lock.count & isize::MAX != 0 {
				immediate = false;
				// Mark as attempting to acquire the lock.
				lock.count |= isize::MIN;
				Err(lock.queue.push(cx.waker().clone(), ()))
			} else {
				// Mark as having acquired the lock.
				lock.count = isize::MIN | 1;
				Ok(())
			}
		})
		.await;
		LockSetExclusiveGuard { lock_set: self, key: MaybeUninit::new(key), immediate }
	}
}

impl<K> Default for LockSet<K> {
	fn default() -> Self {
		Self { locks: Default::default() }
	}
}

pub struct LockSetInclusiveGuard<'a, K: Ord + Eq> {
	lock_set: &'a LockSet<K>,
	key: MaybeUninit<K>,
	immediate: bool,
}

impl<'a, K: Ord + Eq> LockSetInclusiveGuard<'a, K> {
	/// Whether this lock was acquired immediately.
	///
	/// This can be used to determine whether the data structure(s) this lock protects need to be
	/// reloaded.
	pub fn was_immediate(&self) -> bool {
		self.immediate
	}
}

impl<K: Ord + Eq> Drop for LockSetInclusiveGuard<'_, K> {
	fn drop(&mut self) {
		// SAFETY:
		// * The mutable reference will be dropped at the end of this function
		// * We won't acquire another mutable reference.
		let locks = unsafe { &mut *self.lock_set.locks.get() };

		// SAFETY:
		// * Drop runs at most once.
		// * The key is initialized.
		let key = unsafe { self.key.assume_init_read() };

		let mut lock = locks.occupied(key).expect("no lock entry");

		let l = lock.get_mut();
		debug_assert!(l.count & isize::MAX > 0, "lock not inclusively acquired");

		l.count -= 1;
		if l.count & isize::MAX == 0 && l.queue.wake_next().is_none() {
			// No other *sleeping* task is or will attempt to hold this lock, so free space.
			lock.remove();
		}
	}
}

pub struct LockSetExclusiveGuard<'a, K: Ord + Eq> {
	lock_set: &'a LockSet<K>,
	key: MaybeUninit<K>,
	immediate: bool,
}

impl<'a, K: Ord + Eq> LockSetExclusiveGuard<'a, K> {
	/// Whether this lock was acquired immediately.
	///
	/// This can be used to determine whether the data structure(s) this lock protects need to be
	/// reloaded.
	pub fn was_immediate(&self) -> bool {
		self.immediate
	}
}

impl<K: Ord + Eq> Drop for LockSetExclusiveGuard<'_, K> {
	fn drop(&mut self) {
		// SAFETY:
		// * The mutable reference will be dropped at the end of this function
		// * We won't acquire another mutable reference.
		let locks = unsafe { &mut *self.lock_set.locks.get() };

		// SAFETY:
		// * Drop runs at most once.
		// * The key is initialized.
		let key = unsafe { self.key.assume_init_read() };
		let mut lock = locks.occupied(key).expect("no lock entry");

		let l = lock.get_mut();
		debug_assert!(l.count & isize::MIN != 0, "lock not exclusively acquired");

		l.count -= 1;
		l.count &= isize::MAX;

		l.queue.wake_all();

		if l.count == 0 {
			lock.remove();
		}
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
		let s = LockSet::default();
		let a = poll(pin!(s.lock_inclusive(0)));
		assert!(a.is_ready());
		let b = poll(pin!(s.lock_inclusive(0)));
		assert!(b.is_ready());
	}

	#[test]
	fn lock_exclusive() {
		let s = LockSet::default();
		let a = poll(pin!(s.lock_exclusive(0)));
		assert!(a.is_ready());
		let b = poll(pin!(s.lock_exclusive(0)));
		assert!(b.is_pending());
	}

	#[test]
	fn lock_exclusive_distinct() {
		let s = LockSet::default();
		let a = poll(pin!(s.lock_exclusive(0)));
		assert!(a.is_ready());
		let b = poll(pin!(s.lock_exclusive(1)));
		assert!(b.is_ready());
	}

	#[test]
	fn lock_inclusive_exclusive() {
		let s = LockSet::default();
		let a = poll(pin!(s.lock_inclusive(0)));
		assert!(a.is_ready());
		let b = poll(pin!(s.lock_exclusive(0)));
		assert!(b.is_pending());
	}

	#[test]
	fn lock_exclusive_inclusive() {
		let s = LockSet::default();
		let a = poll(pin!(s.lock_exclusive(0)));
		assert!(a.is_ready());
		let b = poll(pin!(s.lock_inclusive(0)));
		assert!(b.is_pending());
	}

	#[test]
	fn lock_ex_inc_release_ex() {
		let s = LockSet::default();
		let a = poll(pin!(s.lock_exclusive(0)));
		assert!(a.is_ready());
		let mut b = pin!(s.lock_inclusive(0));
		assert!(poll(b.as_mut()).is_pending());
		drop(a);
		assert!(poll(b.as_mut()).is_ready());
	}

	#[test]
	fn lock_inc_ex_release_inc() {
		let s = LockSet::default();
		let a = poll(pin!(s.lock_inclusive(0)));
		assert!(a.is_ready());
		let mut b = pin!(s.lock_exclusive(0));
		assert!(poll(b.as_mut()).is_pending());
		drop(a);
		assert!(poll(b.as_mut()).is_ready());
	}

	#[test]
	fn lock_inc_inc_ex_release_inc() {
		let s = LockSet::default();

		let a = poll(pin!(s.lock_inclusive(0)));
		assert!(a.is_ready());
		let b = poll(pin!(s.lock_inclusive(0)));
		assert!(b.is_ready());

		let mut c = pin!(s.lock_exclusive(0));
		assert!(poll(c.as_mut()).is_pending());
		drop(a);
		assert!(poll(c.as_mut()).is_pending());
		drop(b);
		assert!(poll(c.as_mut()).is_ready());
	}
}
