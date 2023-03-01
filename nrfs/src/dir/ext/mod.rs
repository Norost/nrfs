pub mod mtime;
pub mod unix;

use {
	super::{Dir, Index},
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Set `unix` extension data.
	pub(super) async fn ext_set_unix(
		&self,
		index: Index,
		unix: &unix::Entry,
	) -> Result<bool, Error<D>> {
		trace!("ext_set_unix {:?} {:?}", index, unix);
		let unix_offset = self.fs.dir_data(self.id).unix_offset();
		if let Some(o) = unix_offset {
			self.item_set(index, o, &unix.into_raw())
				.await
				.map(|_| true)
		} else {
			Ok(false)
		}
	}

	/// Set `mtime` extension data.
	pub(super) async fn ext_set_mtime(
		&self,
		index: Index,
		mtime: &mtime::Entry,
	) -> Result<bool, Error<D>> {
		trace!("ext_set_mtime {:?} {:?}", index, mtime);
		let mtime_offset = self.fs.dir_data(self.id).mtime_offset();
		if let Some(o) = mtime_offset {
			self.item_set(index, o, &mtime.into_raw())
				.await
				.map(|_| true)
		} else {
			Ok(false)
		}
	}
}
