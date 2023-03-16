use {
	crate::{dir::Dir, item::ItemData, Dev, DirKey, Error, ItemExt, Name, Nrfs, TransferError},
	core::fmt,
};

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

/// Key to a file.
///
/// Can be paired with a [`Nrfs`] object to create a [`File`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileKey {
	/// High ID of parent directory.
	dir_hi: u32,
	/// Low ID of parent directory.
	dir_lo: u32,
	/// Index to corresponding item *data* in parent directory.
	/// The key does *not* point to the name but the *data* of the item.
	pub(crate) index: u32,
}

impl FileKey {
	pub(crate) fn new(dir: u64, index: u32) -> Self {
		Self { dir_hi: (dir >> 32) as u32, dir_lo: dir as u32, index }
	}

	/// Get ID of parent directory.
	pub(crate) fn dir(&self) -> u64 {
		u64::from(self.dir_hi) << 32 | u64::from(self.dir_lo)
	}

	/// Set ID of parent directory.
	pub(crate) fn set_dir(&mut self, id: u64) {
		self.dir_hi = (id >> 32) as u32;
		self.dir_lo = id as u32;
	}
}

/// Helper structure for working with files.
#[derive(Debug)]
pub struct File<'a, D: Dev> {
	/// The filesystem containing the file's data.
	fs: &'a Nrfs<D>,
	/// Key to file.
	key: FileKey,
}

impl<'a, D: Dev> File<'a, D> {
	/// Create a [`File`] helper structure.
	pub fn new(fs: &'a Nrfs<D>, key: FileKey) -> Self {
		Self { fs, key }
	}

	/// Read data.
	///
	/// The returned value indicates how many bytes were actually read.
	pub async fn read(&self, offset: u64, mut buf: &mut [u8]) -> Result<usize, Error<D>> {
		trace!("read {} (len: {})", offset, buf.len());
		if buf.is_empty() {
			return Ok(0);
		}

		let dir = self.dir();
		let item = dir.item_get_data(self.key.index).await?;
		let end = offset + u64::try_from(buf.len()).unwrap();

		if offset >= item.len {
			return Ok(0);
		}
		if end > item.len {
			buf = &mut buf[..usize::try_from(item.len - offset).unwrap()];
		}

		match item.ty {
			ItemData::TY_FILE | ItemData::TY_SYM => {
				let obj = self.fs.get(item.id_or_offset);
				obj.read(offset, buf).await?;
			}
			ItemData::TY_EMBED_FILE | ItemData::TY_EMBED_SYM => {
				let hdr = self.dir().header().await?;
				let heap = self.dir().heap(&hdr);
				heap.read(item.id_or_offset + offset, buf).await?;
			}
			_ => todo!(),
		}

		Ok(buf.len())
	}

	/// Write data in-place.
	///
	/// # Note
	///
	/// This does not check bounds!
	async fn write_inplace(
		&self,
		offset: u64,
		data: &[u8],
		item: ItemData,
	) -> Result<(), Error<D>> {
		trace!("write_inplace {}+{} {:?}", offset, data.len(), &item);
		match item.ty {
			ItemData::TY_FILE | ItemData::TY_SYM => {
				let obj = self.fs.get(item.id_or_offset);
				obj.write(offset, data).await?;
			}
			ItemData::TY_EMBED_FILE | ItemData::TY_EMBED_SYM => {
				let offt = item.id_or_offset + offset;
				let hdr = self.dir().header().await?;
				self.dir().heap(&hdr).write(offt, data).await?;
			}
			_ => todo!(),
		}
		Ok(())
	}

	/// Write data, replacing the tail and growing the embedded file.
	///
	/// Sets the new total length of the file in `item` as well as other properties.
	///
	/// # Note
	///
	/// This does not check bounds!
	async fn write_embed_replace_tail(
		&self,
		offset: u64,
		data: &[u8],
		item: &mut ItemData,
	) -> Result<(), Error<D>> {
		trace!(
			"write_embed_replace_tail {}+{} {:?}",
			offset,
			data.len(),
			item
		);
		let dir = self.dir();
		let keep_len = offset.min(item.len);
		let new_len = offset + u64::try_from(data.len()).unwrap();

		let mut buf = vec![0; keep_len.try_into().unwrap()];
		let mut hdr = dir.header().await?;
		let heap = dir.heap(&hdr);
		heap.read(item.id_or_offset, &mut buf).await?;
		heap.dealloc(&mut hdr, item.id_or_offset, item.len).await?;

		if new_len <= self.embed_factor() {
			item.id_or_offset = heap.alloc(&mut hdr, new_len).await?;
			heap.write(item.id_or_offset, &buf).await?;
			heap.write(item.id_or_offset + offset, data).await?;
		} else {
			let obj = self.fs.storage.create().await?;
			obj.write(0, &buf).await?;
			obj.write(offset, data).await?;
			item.ty = match item.ty {
				ItemData::TY_EMBED_FILE => ItemData::TY_FILE,
				ItemData::TY_EMBED_SYM => ItemData::TY_SYM,
				_ => unreachable!(),
			};
			item.id_or_offset = obj.id();
		};
		dir.set_header(hdr).await?;
		Ok(())
	}

	/// Write data.
	///
	/// The returned value indicates how many bytes were actually written.
	pub async fn write(&self, offset: u64, mut data: &[u8]) -> Result<usize, Error<D>> {
		trace!("write {} (len: {})", offset, data.len());
		assert!(!self.fs.read_only, "read only");
		if data.is_empty() {
			return Ok(0);
		}

		let dir = self.dir();
		let item = dir.item_get_data(self.key.index).await?;
		let end = offset + u64::try_from(data.len()).unwrap();

		if offset >= item.len {
			return Ok(0);
		}
		if end > item.len {
			data = &data[..usize::try_from(item.len - offset).unwrap()];
		}

		self.write_inplace(offset, data, item).await?;

		Ok(data.len())
	}

	/// Write an exact amount of data,
	/// growing the object if necessary.
	pub async fn write_grow(
		&self,
		offset: u64,
		data: &[u8],
	) -> Result<Result<(), LengthTooLong>, Error<D>> {
		trace!("write_grow {} (len: {})", offset, data.len());
		assert!(!self.fs.read_only, "read only");
		if data.is_empty() {
			return Ok(Ok(()));
		}

		let end = offset + u64::try_from(data.len()).unwrap();
		if end > self.fs.storage.obj_max_len() {
			return Ok(Err(LengthTooLong));
		}

		let dir = self.dir();
		let mut item = dir.item_get_data(self.key.index).await?;
		if end <= item.len {
			self.write_inplace(offset, data, item).await?;
			return Ok(Ok(()));
		}

		match item.ty {
			ItemData::TY_FILE | ItemData::TY_SYM => {
				let obj = self.fs.get(item.id_or_offset);
				obj.write(offset, data).await?;
			}
			ItemData::TY_EMBED_FILE | ItemData::TY_EMBED_SYM => {
				self.write_embed_replace_tail(offset, data, &mut item)
					.await?;
			}
			ty => todo!("bad ty {:?}", ty),
		}

		item.len = end;
		dir.item_set_data(self.key.index, item).await?;

		Ok(Ok(()))
	}

	/// Resize the file.
	pub async fn resize(&self, new_len: u64) -> Result<Result<(), LengthTooLong>, Error<D>> {
		trace!("resize {}", new_len);
		assert!(!self.fs.read_only, "read only");

		if new_len > self.fs.storage.obj_max_len() {
			return Ok(Err(LengthTooLong));
		}

		let dir = self.dir();
		let mut item = dir.item_get_data(self.key.index).await?;
		if item.len == new_len {
			return Ok(Ok(()));
		}

		match item.ty {
			ItemData::TY_FILE | ItemData::TY_SYM => {
				if new_len == 0 {
					// Just destroy the object and mark ourselves as embedded again.
					item.ty = match item.ty {
						ItemData::TY_FILE => ItemData::TY_EMBED_FILE,
						ItemData::TY_SYM => ItemData::TY_EMBED_SYM,
						_ => unreachable!(),
					};
					self.fs.get(item.id_or_offset).dealloc().await?;
				} else if new_len < item.len {
					// TODO consider re-embedding.
					let obj = self.fs.get(item.id_or_offset);
					obj.write_zeros(new_len, item.len - new_len).await?;
				}
			}
			ItemData::TY_EMBED_FILE | ItemData::TY_EMBED_SYM => {
				if new_len <= item.len {
					let mut hdr = dir.header().await?;
					dir.heap(&hdr)
						.dealloc(&mut hdr, item.id_or_offset + new_len, item.len - new_len)
						.await?;
					dir.set_header(hdr).await?;
				} else {
					self.write_embed_replace_tail(new_len, &[], &mut item)
						.await?;
				}
			}
			_ => todo!(),
		}

		item.len = new_len;
		dir.item_set_data(self.key.index, item).await?;

		Ok(Ok(()))
	}

	pub async fn len(&self) -> Result<u64, Error<D>> {
		trace!("len");
		Ok(self.dir().item_get_data(self.key.index).await?.len)
	}

	pub async fn is_embed(&self) -> Result<bool, Error<D>> {
		trace!("is_embed");
		let ty = self.dir().item_get_data(self.key.index).await?.ty;
		Ok(matches!(
			ty,
			ItemData::TY_EMBED_FILE | ItemData::TY_EMBED_SYM
		))
	}

	pub async fn ext(&self) -> Result<ItemExt, Error<D>> {
		trace!("ext");
		let hdr = self.dir().header().await?;
		Ok(self.dir().item_get_data_ext(&hdr, self.key.index).await?.1)
	}

	/// Create stub dir helper.
	///
	/// # Note
	///
	/// This doesn't set a valid parent directory & index.
	fn dir(&self) -> Dir<'a, D> {
		Dir::new(self.fs, DirKey::inval(self.key.dir()))
	}

	/// Destroy this file.
	pub async fn destroy(self) -> Result<(), Error<D>> {
		trace!("destroy {:?}", self.key);
		assert!(!self.fs.read_only, "read only");
		let mut hdr = self.dir().header().await?;
		self.dir().item_destroy(&mut hdr, self.key.index).await?;
		self.dir().set_header(hdr).await?;
		Ok(())
	}

	/// Transfer this file.
	pub async fn transfer(
		&mut self,
		to_dir: &Dir<'a, D>,
		to_name: &Name,
	) -> Result<Result<FileKey, TransferError>, Error<D>> {
		trace!(
			"transfer {:?} -> {:#x} {:?}",
			self.key,
			to_dir.key.id,
			to_name
		);
		assert!(!self.fs.read_only, "read only");
		let to_index = match self
			.dir()
			.item_transfer(self.key.index, to_dir, to_name)
			.await?
		{
			Ok(i) => i,
			Err(e) => return Ok(Err(e)),
		};
		self.key.set_dir(to_dir.key.id);
		self.key.index = to_index;
		Ok(Ok(self.key))
	}

	pub fn into_key(self) -> FileKey {
		self.key
	}

	/// Determine the embed factor.
	fn embed_factor(&self) -> u64 {
		let embed_lim = EMBED_FACTOR << self.fs.block_size().to_raw();
		u64::from(u16::MAX).min(embed_lim)
	}
}

/// Error returned if the length is larger than supported.
#[derive(Clone, Debug)]
pub struct LengthTooLong;

impl fmt::Display for LengthTooLong {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		"length too long".fmt(f)
	}
}

impl core::error::Error for LengthTooLong {}
