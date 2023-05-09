use {
	crate::{Dev, ItemKey, Nrfs},
	util::task::lock_set::{LockSetExclusiveGuard, LockSetInclusiveGuard},
};

impl<D: Dev> Nrfs<D> {
	pub(crate) async fn lock_dir(&self, id: u64) -> LockSetInclusiveGuard<'_, u64> {
		self.dir_locks.lock_inclusive(id).await
	}

	pub(crate) async fn lock_dir_mut(&self, id: u64) -> LockSetExclusiveGuard<'_, u64> {
		self.dir_locks.lock_exclusive(id).await
	}

	pub(crate) async fn lock_item(&self, key: ItemKey) -> LockSetInclusiveGuard<'_, ItemKey> {
		self.item_locks.lock_inclusive(key).await
	}

	pub(crate) async fn lock_item_mut(&self, key: ItemKey) -> LockSetExclusiveGuard<'_, ItemKey> {
		self.item_locks.lock_exclusive(key).await
	}
}
