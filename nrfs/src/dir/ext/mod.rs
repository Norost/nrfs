pub mod mtime;
pub mod unix;

use {
	super::Dir,
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	pub(super) async fn ext_set_unix(
		&mut self,
		index: u32,
		unix: unix::Entry,
	) -> Result<bool, Error<D>> {
		if let Some(o) = self.unix_offset {
			self.hashmap()
				.set_raw(index, o, &unix.into_raw())
				.await
				.map(|_| true)
		} else {
			Ok(false)
		}
	}

	pub(super) async fn ext_set_mtime(
		&mut self,
		index: u32,
		mtime: mtime::Entry,
	) -> Result<bool, Error<D>> {
		if let Some(o) = self.mtime_offset {
			self.hashmap()
				.set_raw(index, o, &mtime.into_raw())
				.await
				.map(|_| true)
		} else {
			Ok(false)
		}
	}
}
