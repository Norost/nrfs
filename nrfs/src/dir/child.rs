use {
	crate::{DataHeader, Dev, Idx, Nrfs},
	core::cell::RefMut,
};

/// A file or directory that is a child of another directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Child {
	File(Idx),
	Dir(u64),
}

impl Child {
	/// Get the [`DataHeader`].
	pub fn header<'a, D: Dev>(&self, fs: &'a Nrfs<D>) -> RefMut<'a, DataHeader> {
		match self {
			&Self::File(idx) => RefMut::map(fs.file_data(idx), |d| &mut d.header),
			&Self::Dir(id) => RefMut::map(fs.dir_data(id), |d| &mut d.header),
		}
	}
}
