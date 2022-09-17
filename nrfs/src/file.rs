use crate::{Error, Nrfs, Storage};

#[derive(Debug)]
pub struct File {
	id: u64,
}

impl File {
	pub(crate) fn from_raw(id: u64) -> Self {
		Self { id }
	}

	pub fn read<S>(
		&mut self,
		fs: &mut Nrfs<S>,
		offset: u64,
		buf: &mut [u8],
	) -> Result<usize, Error<S>>
	where
		S: Storage,
	{
		fs.read(self.id, offset, buf)
	}

	pub fn read_exact<S>(
		&mut self,
		fs: &mut Nrfs<S>,
		offset: u64,
		buf: &mut [u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		fs.read_exact(self.id, offset, buf)
	}

	pub fn write<S>(
		&mut self,
		fs: &mut Nrfs<S>,
		offset: u64,
		data: &[u8],
	) -> Result<usize, Error<S>>
	where
		S: Storage,
	{
		fs.write(self.id, offset, data)
	}

	pub fn write_all<S>(
		&mut self,
		fs: &mut Nrfs<S>,
		offset: u64,
		data: &[u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		fs.write_all(self.id, offset, data)
	}
}
