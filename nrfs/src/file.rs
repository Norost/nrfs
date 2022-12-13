use {
	crate::{dir::Type, Dev, Dir, Error},
	core::fmt,
};

pub struct File<'a, 'b, D: Dev> {
	dir: &'b mut Dir<'a, D>,
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

impl<'a, 'b, D: Dev> File<'a, 'b, D> {
	pub(crate) fn from_obj(dir: &'b mut Dir<'a, D>, is_sym: bool, id: u64, index: u32) -> Self {
		Self { dir, inner: Inner::Object { id, index }, is_sym }
	}

	pub(crate) fn from_embed(
		dir: &'b mut Dir<'a, D>,
		is_sym: bool,
		index: u32,
		offset: u64,
		length: u16,
	) -> Self {
		Self { dir, inner: Inner::Embed { index, offset, length }, is_sym }
	}

	pub async fn read(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.read(*id, offset, buf).await,
			Inner::Embed { offset: offt, length, .. } => {
				let l = u64::from(*length).saturating_sub(offset);
				let l = buf.len().min(l as usize);
				let buf = &mut buf[..l];
				self.dir.read_heap(offt + offset, buf).await.map(|_| l)
			}
		}
	}

	pub async fn read_exact(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), Error<D>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.read_exact(*id, offset, buf).await,
			Inner::Embed { offset: offt, length, .. } => {
				let l = u64::from(*length).saturating_sub(offset);
				let l = buf.len().min(l as usize);
				let buf = &mut buf[..l];
				self.dir.read_heap(offt + offset, buf).await
			}
		}
	}

	pub async fn write(&mut self, offset: u64, data: &[u8]) -> Result<usize, Error<D>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.write(*id, offset, data).await,
			Inner::Embed { offset: offt, length, index: _ } => {
				if offset >= u64::from(*length) {
					return Ok(0);
				}
				let data = &data[..data.len().min(usize::from(*length) - offset as usize)];
				self.dir.write_heap(*offt + offset, data).await?;
				Ok(data.len())
			}
		}
	}

	pub async fn write_all(&mut self, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.write_all(*id, offset, data).await,
			Inner::Embed { offset: offt, length, index: _ } => {
				if offset >= u64::from(*length) {
					return Err(Error::Truncated);
				}
				let data = &data[..data.len().min(usize::from(*length) - offset as usize)];
				self.dir.write_heap(*offt + offset, data).await?;
				Ok(())
			}
		}
	}

	pub async fn write_grow(&mut self, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		match &mut self.inner {
			Inner::Object { id, .. } => self.dir.fs.write_grow(*id, offset, data).await,
			Inner::Embed { offset: offt, length, index } => {
				let index = *index;
				let end = offset + data.len() as u64;

				// Avoid reallocation if the data fits inside the current allocation.
				if end < u64::from(*length) {
					return self.write_all(offset, data).await;
				}

				// Take data off the directory's heap and deallocate.
				let mut buf = vec![0; usize::from(*length)];
				self.dir.read_heap(*offt, &mut buf).await?;
				self.dir.dealloc(*offt, u64::from(*length)).await?;

				// Determine whether we should keep the data embedded.
				let bs = 1u64 << self.dir.fs.block_size();
				if end <= u64::from(u16::MAX).min(bs * EMBED_FACTOR) {
					let o = self.dir.alloc(end).await?;
					// TODO avoid redundant tail write
					self.dir.write_heap(o, &buf).await?;
					self.dir.write_heap(o + offset, &data).await?;
					*offt = o;
					*length = end as _;
				} else {
					// Create object, copy existing & new data to it.
					let obj = self.dir.fs.storage.create().await?;
					obj.resize(end).await?;
					// TODO ditto
					obj.write(0, &buf).await?;
					obj.write(offset, data).await?;
					self.inner = Inner::Object { id: obj.id(), index };
				}
				self.dir.set_ty(index, self.ty()).await
			}
		}
	}

	pub async fn resize(&mut self, new_len: u64) -> Result<(), Error<D>> {
		match &mut self.inner {
			Inner::Object { index, id } if new_len == 0 => {
				self.dir.fs.storage.decr_ref(*id).await?;
				self.inner = Inner::Embed { index: *index, offset: 0, length: 0 };
				Ok(())
			}
			// TODO consider re-embedding.
			Inner::Object { id, .. } => self.dir.fs.resize(*id, new_len).await,
			Inner::Embed { length, .. } if u64::from(*length) == new_len => Ok(()),
			Inner::Embed { offset: offt, length, index } => {
				let index = *index;

				// Take the (minimum amount of) data off the directory's heap.
				let mut buf = vec![0; new_len.min(u64::from(*length)) as _];
				self.dir.read_heap(*offt, &mut buf).await?;
				self.dir.dealloc(*offt, u64::from(*length)).await?;

				// Determine whether we should keep the data embedded.
				let bs = 1u64 << self.dir.fs.block_size();
				if new_len <= u64::from(u16::MAX).min(bs * EMBED_FACTOR) {
					// Keep it embedded, write to
					let o = self.dir.alloc(new_len).await?;
					self.dir.write_heap(o, &buf).await?;
					*offt = o;
					*length = new_len as _;
				} else {
					let obj = self.dir.fs.storage.create().await?;
					obj.resize(new_len).await?;
					obj.write(0, &buf).await?;
					self.inner = Inner::Object { id: obj.id(), index };
				}
				self.dir.set_ty(index, self.ty()).await
			}
		}
	}

	fn ty(&self) -> Type {
		match &self.inner {
			Inner::Object { id, .. } if self.is_sym => Type::Sym { id: *id },
			Inner::Object { id, .. } => Type::File { id: *id },
			Inner::Embed { offset, length, .. } if self.is_sym => {
				Type::EmbedSym { offset: *offset, length: *length }
			}
			Inner::Embed { offset, length, .. } => {
				Type::EmbedFile { offset: *offset, length: *length }
			}
		}
	}

	pub async fn len(&mut self) -> Result<u64, Error<D>> {
		match &self.inner {
			Inner::Object { id, .. } => self.dir.fs.length(*id).await,
			Inner::Embed { length, .. } => Ok((*length).into()),
		}
	}
}

impl<D: Dev> fmt::Debug for File<'_, '_, D>
where
	for<'a> Dir<'a, D>: fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(File))
			.field("dir", &self.dir)
			.field("is_sym", &self.is_sym)
			.field("inner", &self.inner)
			.finish()
	}
}
