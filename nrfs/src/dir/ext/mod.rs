pub mod mtime;
pub mod unix;

use {
	super::Dir,
	crate::{Error, Storage},
};

impl<'a, S: Storage> Dir<'a, S> {
	pub(super) fn ext_set_unix(&mut self, index: u32, unix: unix::Entry) -> Result<bool, Error<S>> {
		self.unix_offset
			.map(|o| self.hashmap().set_raw(index, o, &unix.into_raw()))
			.transpose()
			.map(|r| r.is_some())
	}

	pub(super) fn ext_set_mtime(
		&mut self,
		index: u32,
		mtime: mtime::Entry,
	) -> Result<bool, Error<S>> {
		self.mtime_offset
			.map(|o| self.hashmap().set_raw(index, o, &mtime.into_raw()))
			.transpose()
			.map(|r| r.is_some())
	}
}
