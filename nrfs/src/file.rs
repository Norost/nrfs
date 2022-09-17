use crate::{Dir, dir::Type, Error, Nrfs, Storage};

#[derive(Debug)]
pub struct File {
	is_sym: bool,
	inner: Inner,
}

#[derive(Debug)]
enum Inner {
	Object { id: u64, index: u32 },
	Embed { index: u32, offset: u64, length: u16 },
}

impl File {
	pub(crate) fn from_obj(is_sym: bool, id: u64, index: u32) -> Self {
		Self { inner: Inner::Object { id, index }, is_sym }
	}

	pub(crate) fn from_embed(is_sym: bool, index: u32, offset: u64, length: u16) -> Self {
		Self { inner: Inner::Embed { index, offset, length }, is_sym }
	}

	pub fn read<S>(
		&mut self,
		fs: &mut Nrfs<S>,
		dir: &mut Dir,
		offset: u64,
		buf: &mut [u8],
	) -> Result<usize, Error<S>>
	where
		S: Storage,
	{
		match &self.inner {
			Inner::Object { id, .. } => fs.read(*id, offset, buf),
			Inner::Embed { offset: offt, length, .. } => {
				let l = u64::from(*length).saturating_sub(offset);
				let l = buf.len().min(l as usize);
				let buf = &mut buf[..l];
				dir.read_heap(fs, offt + offset, buf).map(|_| l)
			}
		}
	}

	pub fn read_exact<S>(
		&mut self,
		fs: &mut Nrfs<S>,
		dir: &mut Dir,
		offset: u64,
		buf: &mut [u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		match &self.inner {
			Inner::Object { id, .. } => fs.read_exact(*id, offset, buf),
			Inner::Embed { offset: offt, length, .. } => {
				let l = u64::from(*length).saturating_sub(offset);
				let l = buf.len().min(l as usize);
				let buf = &mut buf[..l];
				dir.read_heap(fs, offt + offset, buf)
			}
		}
	}

	pub fn write<S>(
		&mut self,
		fs: &mut Nrfs<S>,
		dir: &mut Dir,
		offset: u64,
		data: &[u8],
	) -> Result<usize, Error<S>>
	where
		S: Storage,
	{
		// Same thing at the moment anyways
		self.write_all(fs, dir, offset, data).map(|()| data.len())
	}

	pub fn write_all<S>(
		&mut self,
		fs: &mut Nrfs<S>,
		dir: &mut Dir,
		offset: u64,
		data: &[u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		match &self.inner {
			Inner::Object { id, .. } => fs.write_all(*id, offset, data),
			Inner::Embed { offset: 0, length: 0, index } => {
				let index = *index;
				let id = fs.storage.new_object().map_err(Error::Nros)?;
				self.inner = Inner::Object { id, index };
				let ty = if self.is_sym { Type::Sym { id } } else { Type::File { id } };
				dir.set_ty(fs, index, ty)?;
				fs.write_all(id, offset, data)
			}
			Inner::Embed { offset: offt, length, .. } => {
				todo!()
			}
		}
	}
}
