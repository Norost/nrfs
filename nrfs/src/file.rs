use {
	crate::{dir::Type, Dir, Error, Nrfs, Storage},
	core::fmt,
};

pub struct File<'a, 'b, S: Storage> {
	dir: &'b mut Dir<'a, S>,
	is_sym: bool,
	inner: Inner,
}

#[derive(Debug)]
enum Inner {
	Object { id: u64, index: u32 },
	Embed { index: u32, offset: u64, length: u16 },
}

impl<'a, 'b, S: Storage> File<'a, 'b, S> {
	pub(crate) fn from_obj(dir: &'b mut Dir<'a, S>, is_sym: bool, id: u64, index: u32) -> Self {
		Self { dir, inner: Inner::Object { id, index }, is_sym }
	}

	pub(crate) fn from_embed(
		dir: &'b mut Dir<'a, S>,
		is_sym: bool,
		index: u32,
		offset: u64,
		length: u16,
	) -> Self {
		Self { dir, inner: Inner::Embed { index, offset, length }, is_sym }
	}

	pub fn read(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize, Error<S>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.read(*id, offset, buf),
			Inner::Embed { offset: offt, length, .. } => {
				let l = u64::from(*length).saturating_sub(offset);
				let l = buf.len().min(l as usize);
				let buf = &mut buf[..l];
				self.dir.read_heap(offt + offset, buf).map(|_| l)
			}
		}
	}

	pub fn read_exact(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), Error<S>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.read_exact(*id, offset, buf),
			Inner::Embed { offset: offt, length, .. } => {
				let l = u64::from(*length).saturating_sub(offset);
				let l = buf.len().min(l as usize);
				let buf = &mut buf[..l];
				self.dir.read_heap(offt + offset, buf)
			}
		}
	}

	pub fn write(&mut self, offset: u64, data: &[u8]) -> Result<usize, Error<S>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.write(*id, offset, data),
			Inner::Embed { offset: 0, length: 0, index } => {
				let id = self.empty_to_object(*index)?;
				self.dir.fs.write(id, offset, data)
			}
			Inner::Embed { offset: offt, length, .. } => {
				todo!()
			}
		}
	}

	pub fn write_all(&mut self, offset: u64, data: &[u8]) -> Result<(), Error<S>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.write_all(*id, offset, data),
			Inner::Embed { offset: 0, length: 0, index } => {
				let id = self.empty_to_object(*index)?;
				self.dir.fs.write_all(id, offset, data)
			}
			Inner::Embed { offset: offt, length, .. } => {
				todo!()
			}
		}
	}

	pub fn write_grow(&mut self, offset: u64, data: &[u8]) -> Result<(), Error<S>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.write_grow(*id, offset, data),
			Inner::Embed { offset: 0, length: 0, index } => {
				let id = self.empty_to_object(*index)?;
				self.dir.fs.write_grow(id, offset, data)
			}
			Inner::Embed { offset: offt, length, .. } => {
				todo!()
			}
		}
	}

	fn empty_to_object(&mut self, index: u32) -> Result<u64, Error<S>> {
		let id = self.dir.fs.storage.new_object().map_err(Error::Nros)?;
		self.inner = Inner::Object { id, index };
		let ty = if self.is_sym {
			Type::Sym { id }
		} else {
			Type::File { id }
		};
		self.dir.set_ty(index, ty)?;
		Ok(id)
	}

	pub fn len(&mut self) -> Result<u64, Error<S>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.storage.object_len(*id).map_err(Error::Nros),
			Inner::Embed { length, .. } => Ok((*length).into()),
		}
	}
}

impl<S: Storage> fmt::Debug for File<'_, '_, S>
where
	for<'a> Dir<'a, S>: fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(File))
			.field("dir", &self.dir)
			.field("is_sym", &self.is_sym)
			.field("inner", &self.inner)
			.finish()
	}
}
