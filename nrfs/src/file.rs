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
	Object { index: u32, id: u64 },
	Embed { index: u32, offset: u64, length: u16 },
}

/// How many multiples of the block size a file should be before it is unembedded.
///
/// Waste calculation: `waste = 1 - blocks / (blocks + 1)`
///
/// Some factors for reference:
///
/// +--------+------------------------------+--------------------+-------------------+
/// | Factor | Maximum waste (Uncompressed) | Maximum size (512) | Maximum size (4K) |
/// +========+==============================+====================+===================+
/// |      1 |                          50% |                512 |                4K |
/// +--------+------------------------------+--------------------+-------------------+
/// |      2 |                          33% |                 1K |                8K |
/// +--------+------------------------------+--------------------+-------------------+
/// |      3 |                          25% |               1.5K |               12K |
/// +--------+------------------------------+--------------------+-------------------+
/// |      4 |                          20% |                 2K |               16K |
/// +--------+------------------------------+--------------------+-------------------+
/// |      5 |                        16.6% |               2.5K |               20K |
/// +--------+------------------------------+--------------------+-------------------+
///
/// * Maximum waste = how much data may be padding if stored as an object.
const EMBED_FACTOR: u64 = 4;

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
			Inner::Embed { offset: offt, length, index } => {
				if offset >= u64::from(*length) {
					return Ok(0);
				}
				let data = &data[..data.len().min(usize::from(*length) - offset as usize)];
				self.dir.write_heap(*offt + offset, data)?;
				Ok(data.len())
			}
		}
	}

	pub fn write_all(&mut self, offset: u64, data: &[u8]) -> Result<(), Error<S>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.write_all(*id, offset, data),
			Inner::Embed { offset: offt, length, index } => {
				if offset >= u64::from(*length) {
					return Err(Error::Truncated);
				}
				let data = &data[..data.len().min(usize::from(*length) - offset as usize)];
				self.dir.write_heap(*offt + offset, data)?;
				Ok(())
			}
		}
	}

	pub fn write_grow(&mut self, offset: u64, data: &[u8]) -> Result<(), Error<S>> {
		match &mut self.inner {
			Inner::Object { id, .. } => self.dir.fs.write_grow(*id, offset, data),
			Inner::Embed { offset: offt, length, index } => {
				let index = *index;
				let end = offset + data.len() as u64;

				// Avoid reallocation if the data fits inside the current allocation.
				if end < u64::from(*length) {
					return self.write_all(offset, data);
				}

				let mut buf = vec![0; usize::from(*length)];
				self.dir.read_heap(*offt, &mut buf)?;
				self.dir.dealloc(*offt, u64::from(*length))?;
				// Determine whether we should keep the data embedded.
				let bs = 1 << self.dir.fs.storage().block_size_p2();
				if end <= u64::from(u16::MAX).min(bs * EMBED_FACTOR) {
					let o = self.dir.alloc(end)?;
					// TODO avoid redundant tail write
					self.dir.write_heap(o, &buf)?;
					self.dir.write_heap(o + offset, &data)?;
					*offt = o;
					*length = end as _;
				} else {
					let id = self.dir.fs.storage.new_object().map_err(Error::Nros)?;
					self.dir.fs.resize(id, end)?;
					// TODO ditto
					self.dir.fs.write_all(id, 0, &buf)?;
					self.dir.fs.write_all(id, offset, data)?;
					self.inner = Inner::Object { id, index };
				}
				self.dir.set_ty(index, self.ty())
			}
		}
	}

	pub fn resize(&mut self, new_len: u64) -> Result<(), Error<S>> {
		match &mut self.inner {
			Inner::Object { index, id } if new_len == 0 => {
				self.dir.fs.storage.decr_ref(*id).map_err(Error::Nros)?;
				self.inner = Inner::Embed { index: *index, offset: 0, length: 0 };
				Ok(())
			}
			Inner::Object { id, .. } => self.dir.fs.resize(*id, new_len),
			Inner::Embed { length, .. } if u64::from(*length) == new_len => Ok(()),
			Inner::Embed { offset: offt, length, index } => {
				let index = *index;
				let mut buf = vec![0; new_len.min(u64::from(*length)) as _];
				self.dir.read_heap(*offt, &mut buf)?;
				self.dir.dealloc(*offt, u64::from(*length))?;
				// Determine whether we should keep the data embedded.
				let bs = 1 << self.dir.fs.storage().block_size_p2();
				if new_len <= u64::from(u16::MAX).min(bs * EMBED_FACTOR) {
					let o = self.dir.alloc(new_len)?;
					self.dir.write_heap(o, &buf)?;
					*offt = o;
					*length = new_len as _;
				} else {
					let id = self.dir.fs.storage.new_object().map_err(Error::Nros)?;
					self.dir.fs.resize(id, new_len)?;
					self.dir.fs.write_all(id, 0, &buf)?;
					self.inner = Inner::Object { id, index };
				}
				self.dir.set_ty(index, self.ty())
			}
		}
	}

	fn ty(&self) -> Type {
		match &self.inner {
			Inner::Object { id, ..} if self.is_sym => Type::Sym { id: *id },
			Inner::Object { id, ..} => Type::File { id: *id },
			Inner::Embed { offset, length, ..} if self.is_sym => Type::EmbedSym {
				offset: *offset,
				length: *length,
			},
			Inner::Embed { offset, length, ..} => Type::EmbedFile {
				offset: *offset,
				length: *length,
			},
		}
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
