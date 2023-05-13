use {
	crate::{Dev, Nrfs},
	util::task::lock_set::{LockSetExclusiveGuard, LockSetInclusiveGuard},
};

impl<D: Dev> Nrfs<D> {
	pub(crate) async fn lock_dir(&self, id: u64) -> LockSetInclusiveGuard<'_, u64> {
		self.dir_locks.lock_inclusive(id).await
	}

	pub(crate) async fn lock_dir_mut(&self, id: u64) -> LockSetExclusiveGuard<'_, u64> {
		self.dir_locks.lock_exclusive(id).await
	}
}
