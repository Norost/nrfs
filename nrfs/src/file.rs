use {
	crate::{
		dir::{Child, Dir, Type},
		DataHeader, Dev, DirRef, Error, FileRef, Idx, Nrfs, SymRef, UnknownRef,
	},
	core::{cell::RefMut, mem},
	std::collections::hash_map,
};

/// [`File`] shared mutable data.
#[derive(Debug)]
pub(crate) struct FileData {
	/// Data header.
	pub(crate) header: DataHeader,
	/// Reference to file data, which may be a separate object or embedded on a directory's heap.
	pub(crate) inner: Inner,
	/// Whether this file has been removed and the corresponding item is dangling.
	pub(crate) is_dangling: bool,
}

#[derive(Debug)]
pub(crate) enum Inner {
	/// The data is in a separate object.
	Object { id: u64 },
	/// The data is embedded on the parent directory's heap.
	Embed { offset: u64, length: u16 },
}

impl FileData {
	/// Get the directory type of this file.
	fn ty(&self, ty: Ty) -> Type {
		match (ty, &self.inner) {
			(Ty::File, &Inner::Object { id }) => Type::File { id },
			(Ty::File, &Inner::Embed { offset, length }) => Type::EmbedFile { offset, length },
			(Ty::Sym, &Inner::Object { id }) => Type::Sym { id },
			(Ty::Sym, &Inner::Embed { offset, length }) => Type::EmbedSym { offset, length },
		}
	}
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

/// Helper structure for working with files.
#[derive(Debug)]
pub(crate) struct File<'a, 'b, D: Dev> {
	/// The filesystem containing the file's data.
	fs: &'a Nrfs<D>,
	/// Background task runner.
	bg: &'b nros::Background<'a, D>,
	/// The index of this file.
	idx: Idx,
	/// What type of file this is.
	ty: Ty,
}

/// File type.
#[derive(Copy, Clone, Debug)]
enum Ty {
	/// Generic file, i.e. blob of arbitrary data.
	File,
	/// Symbolic link
	Sym,
}

impl<'a, 'b, D: Dev> File<'a, 'b, D> {
	/// Create a [`File`] helper structure.
	fn new(fs: &'a Nrfs<D>, bg: &'b nros::Background<'a, D>, idx: Idx, ty: Ty) -> Self {
		Self { fs, bg, idx, ty }
	}

	/// Read data.
	///
	/// The returned value indicates how many bytes were actually read.
	async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
		trace!("read {} (len: {})", offset, buf.len());
		if buf.is_empty() {
			return Ok(0);
		}
		let data_f = self.fs.file_data(self.idx);
		let dir = Dir::new(self.bg, self.fs, data_f.header.parent_id);

		match &data_f.inner {
			&Inner::Object { id } => {
				drop(data_f);
				let len = self
					.fs
					.storage
					.get(self.bg, id)
					.await?
					.read(offset, buf)
					.await?;
				Ok(len)
			}
			&Inner::Embed { offset: offt, length } => {
				drop(data_f);
				// If the offset extends past the end, don't even bother.
				let length = u64::try_from(length).unwrap();
				if offset >= length {
					return Ok(0);
				}
				// Truncate buffer so we don't read out-of-bounds.
				let l = usize::try_from(length - offset).unwrap();
				let l = buf.len().min(l);
				let buf = &mut buf[..l];
				// Read from directory heap
				dir.read_heap(offt + offset, buf).await.map(|_| l)
			}
		}
	}

	/// Read an exact amount of data.
	///
	/// If the buffer cannot be filled an error is returned.
	async fn read_exact(&self, offset: u64, buf: &mut [u8]) -> Result<(), Error<D>> {
		trace!("read_exact {} (len: {})", offset, buf.len());
		if buf.is_empty() {
			return Ok(());
		}
		let data_f = self.fs.file_data(self.idx);
		let dir = Dir::new(self.bg, self.fs, data_f.header.parent_id);

		match &data_f.inner {
			&Inner::Object { id } => {
				drop(data_f);
				let obj = self.fs.storage.get(self.bg, id).await?;
				crate::read_exact(&obj, offset, buf).await
			}
			&Inner::Embed { offset: offt, length } => {
				drop(data_f);
				// If the offset extends past the end, don't even bother.
				let end = offt + u64::from(length);
				if offset >= end {
					return Err(Error::Truncated);
				}
				// Ensure we can fill the buffer completely.
				if u64::try_from(buf.len()).unwrap() > end - offset {
					return Err(Error::Truncated);
				}
				// Read from directory heap
				dir.read_heap(offt + offset, buf).await
			}
		}
	}

	/// Write data.
	///
	/// The returned value indicates how many bytes were actually written.
	async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, Error<D>> {
		trace!("write {} (len: {})", offset, data.len());
		if data.is_empty() {
			return Ok(0);
		}
		let data_f = self.fs.file_data(self.idx);
		let dir = Dir::new(self.bg, self.fs, data_f.header.parent_id);

		match &data_f.inner {
			&Inner::Object { id } => {
				drop(data_f);
				let len = self
					.fs
					.storage
					.get(&self.bg, id)
					.await?
					.write(offset, data)
					.await?;
				Ok(len)
			}
			&Inner::Embed { offset: offt, length } => {
				drop(data_f);
				if offset >= u64::from(length) {
					return Ok(0);
				}
				let data = &data[..data.len().min(usize::from(length) - offset as usize)];
				dir.write_heap(offt + offset, data).await?;
				Ok(data.len())
			}
		}
	}

	/// Write an exact amount of data.
	///
	/// If not all data could be written an error is returned.
	async fn write_all(&self, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		trace!("write_all {} (len: {})", offset, data.len());
		if data.is_empty() {
			return Ok(());
		}
		let data_f = self.fs.file_data(self.idx);
		let dir = Dir::new(self.bg, self.fs, data_f.header.parent_id);

		match &data_f.inner {
			&Inner::Object { id } => {
				drop(data_f);
				let obj = self.fs.storage.get(&self.bg, id).await?;
				crate::write_all(&obj, offset, data).await
			}
			&Inner::Embed { offset: offt, length } => {
				drop(data_f);
				if offset >= u64::from(length) {
					return Err(Error::Truncated);
				}
				let data = &data[..data.len().min(usize::from(length) - offset as usize)];
				dir.write_heap(offt + offset, data).await?;
				Ok(())
			}
		}
	}

	/// Write an exact amount of data,
	/// growing the object if necessary.
	async fn write_grow(&self, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		trace!("write_grow {} (len: {})", offset, data.len());
		if data.is_empty() {
			return Ok(());
		}
		let data_f = self.fs.file_data(self.idx);
		let dir = Dir::new(self.bg, self.fs, data_f.header.parent_id);

		match &data_f.inner {
			&Inner::Object { id } => {
				drop(data_f);
				let obj = self.fs.storage.get(&self.bg, id).await?;
				crate::write_grow(&obj, offset, data).await
			}
			&Inner::Embed { offset: offt, length } => {
				drop(data_f);
				let end = offset + u64::try_from(data.len()).unwrap();

				// Avoid reallocation if the data fits inside the current allocation.
				if end < u64::from(length) {
					return self.write_all(offset, data).await;
				}

				// Take data off the directory's heap and deallocate.
				let mut buf = vec![0; usize::from(length)];
				dir.read_heap(offt, &mut buf).await?;
				dir.dealloc_heap(offt, u64::from(length)).await?;

				// Determine whether we should keep the data embedded.
				let bs = 1u64 << self.fs.block_size().to_raw();
				let new_inner = if end <= u64::from(u16::MAX).min(bs * EMBED_FACTOR) {
					let o = dir.alloc_heap(end).await?;
					// TODO avoid redundant tail write
					dir.write_heap(o, &buf).await?;
					dir.write_heap(o + offset, &data).await?;
					Inner::Embed { offset: o, length: end.try_into().unwrap() }
				} else {
					// Create object, copy existing & new data to it.
					let obj = self.fs.storage.create(self.bg).await?;
					obj.resize(end).await?;
					// TODO ditto
					obj.write(0, &buf).await?;
					obj.write(offset, data).await?;
					Inner::Object { id: obj.id() }
				};

				let mut data_f = self.fs.file_data(self.idx);
				data_f.inner = new_inner;

				// Update directory entry
				let (index, ty) = (data_f.header.parent_index, data_f.ty(self.ty));
				drop(data_f);
				dir.set_ty(index, ty).await
			}
		}
	}

	/// Resize the file.
	async fn resize(&self, new_len: u64) -> Result<(), Error<D>> {
		trace!("resize {}", new_len);
		let mut data = self.fs.file_data(self.idx);
		let dir = Dir::new(self.bg, self.fs, data.header.parent_id);

		match &data.inner {
			&Inner::Object { id } if new_len == 0 => {
				// Just destroy the object and mark ourselves as embedded aain.
				data.inner = Inner::Embed { offset: 0, length: 0 };
				let (index, ty) = (data.header.parent_index, data.ty(self.ty));
				drop(data);
				dir.set_ty(index, ty).await?;
				self.fs
					.storage
					.get(self.bg, id)
					.await?
					.decrease_reference_count()
					.await?;
				Ok(())
			}
			&Inner::Object { id } => {
				// TODO consider re-embedding.
				drop(data);
				self.fs
					.storage
					.get(self.bg, id)
					.await?
					.resize(new_len)
					.await?;
				Ok(())
			}
			&Inner::Embed { length, .. } if u64::from(length) == new_len => {
				// Don't bother doing anything
				Ok(())
			}
			&Inner::Embed { offset: offt, length } => {
				// Take the (minimum amount of) data off the directory's heap.
				drop(data);
				let mut buf = vec![0; new_len.min(u64::from(length)) as _];
				dir.read_heap(offt, &mut buf).await?;
				dir.dealloc_heap(offt, u64::from(length)).await?;

				// Determine whether we should keep the data embedded.
				let bs = 1u64 << self.fs.block_size().to_raw();
				let new_inner = if new_len <= u64::from(u16::MAX).min(bs * EMBED_FACTOR) {
					// Keep it embedded, write to
					let o = dir.alloc_heap(new_len).await?;
					dir.write_heap(o, &buf).await?;
					Inner::Embed { offset: o, length: new_len.try_into().unwrap() }
				} else {
					// Move to an object.
					let obj = self.fs.storage.create(self.bg).await?;
					obj.resize(new_len).await?;
					obj.write(0, &buf).await?;
					Inner::Object { id: obj.id() }
				};

				let mut data = self.fs.file_data(self.idx);
				data.inner = new_inner;

				// Update directory entry
				let (index, ty) = (data.header.parent_index, data.ty(self.ty));
				drop(data);
				dir.set_ty(index, ty).await
			}
		}
	}

	/// Get the length of this file.
	async fn len(&self) -> Result<u64, Error<D>> {
		let data = self.fs.file_data(self.idx);
		match &data.inner {
			&Inner::Object { id } => {
				drop(data);
				Ok(self.fs.storage.get(self.bg, id).await?.len().await?)
			}
			&Inner::Embed { length, .. } => Ok(length.into()),
		}
	}

	/// Whether this file is embedded or not.
	fn is_embedded(&self) -> bool {
		matches!(&self.fs.file_data(self.idx).inner, Inner::Embed { .. })
	}
}

macro_rules! impl_common {
	($s:ident -> $self:expr, $ty:expr) => {
		/// Read data.
		///
		/// The returned value indicates how many bytes were actually read.
		pub async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
			self.file().read(offset, buf).await
		}

		/// Read an exact amount of data.
		///
		/// If the buffer cannot be filled an error is returned.
		pub async fn read_exact(&self, offset: u64, buf: &mut [u8]) -> Result<(), Error<D>> {
			self.file().read_exact(offset, buf).await
		}

		/// Write data.
		///
		/// The returned value indicates how many bytes were actually written.
		pub async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, Error<D>> {
			self.file().write(offset, data).await
		}

		/// Write an exact amount of data.
		///
		/// If not all data could be written an error is returned.
		pub async fn write_all(&self, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
			self.file().write_all(offset, data).await
		}

		/// Write an exact amount of data,
		/// growing the object if necessary.
		pub async fn write_grow(&self, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
			self.file().write_grow(offset, data).await
		}

		/// Get the length of this file.
		pub async fn len(&self) -> Result<u64, Error<D>> {
			self.file().len().await
		}

		/// Resize the file.
		pub async fn resize(&self, new_len: u64) -> Result<(), Error<D>> {
			self.file().resize(new_len).await
		}

		/// Whether this file is embedded or not.
		pub fn is_embedded(&self) -> bool {
			self.file().is_embedded()
		}

		/// Construct a helper [`File`]
		pub(crate) fn file(&self) -> File<'a, 'b, D> {
			let $s = self;
			let s = &$self;
			File::new(s.fs, s.bg, s.idx, $ty)
		}
	};
}

impl<'a, 'b, D: Dev> FileRef<'a, 'b, D> {
	/// Create a new [`FileRef`] to an object.
	pub(crate) fn from_obj(dir: &Dir<'a, 'b, D>, id: u64, index: u32) -> Self {
		Self::new_ref(dir, Inner::Object { id }, index)
	}

	/// Create a new [`FileRef`] to embedded data.
	pub(crate) fn from_embed(dir: &Dir<'a, 'b, D>, offset: u64, length: u16, index: u32) -> Self {
		Self::new_ref(dir, Inner::Embed { offset, length }, index)
	}

	/// Create a new [`FileRef`].
	fn new_ref(dir: &Dir<'a, 'b, D>, inner: Inner, index: u32) -> Self {
		// Split RefMut so we don't need to drop and reborrow the annoying way.
		let (mut dirs, mut files) = RefMut::map_split(dir.fs.data.borrow_mut(), |data| {
			(&mut data.directories, &mut data.files)
		});

		let dir_data = dirs.get_mut(&dir.id).expect("no DirData with id");
		let idx = match dir_data.children.entry(index) {
			hash_map::Entry::Occupied(e) => match e.get() {
				&Child::Dir(_) => unreachable!("expected File, not Dir"),
				&Child::File(idx) => {
					// Reference existing FileData
					files[idx].header.reference_count += 1;
					idx
				}
			},
			hash_map::Entry::Vacant(e) => {
				// Insert new FileData and reference parent dict
				let idx = files.insert(FileData {
					header: DataHeader::new(dir.id, index),
					inner,
					is_dangling: false,
				});
				e.insert(Child::File(idx));

				dir_data.header.reference_count += 1;

				idx
			}
		};

		Self { fs: dir.fs, bg: dir.bg, idx }
	}

	/// Destroy the reference to this file.
	///
	/// This will perform cleanup if the file is dangling
	/// and this was the last reference.
	pub async fn drop(self) -> Result<(), Error<D>> {
		// Don't run the Drop impl
		let Self { fs, bg, idx } = self;
		mem::forget(self);

		let mut fs_ref = fs.data.borrow_mut();
		let fsr = &mut *fs_ref; // borrow errors ahoy!

		let mut data = fsr.files.get_mut(idx).expect("filedata should be present");

		data.header.reference_count -= 1;
		if data.header.reference_count == 0 {
			// Remove itself from parent directory.
			let dir = fsr
				.directories
				.get_mut(&data.header.parent_id)
				.expect("parent dir is not loaded");
			let r = dir.children.remove(&data.header.parent_index);
			debug_assert!(matches!(r, Some(Child::File(i)) if i == idx));

			// Remove filedata.
			let data = fsr.files.remove(idx).expect("filedata should be present");

			drop(fs_ref);

			// Reconstruct DirRef to parent.
			let dir = DirRef { fs, bg, id: data.header.parent_id };

			// If dangling, destroy associated file data.
			let r = async {
				if data.is_dangling && !fs.read_only {
					match data.inner {
						Inner::Embed { offset, length } => {
							dir.dir().dealloc_heap(offset, length.into()).await?
						}
						Inner::Object { id } => {
							fs.storage
								.get(bg, id)
								.await?
								.decrease_reference_count()
								.await?
						}
					}
					dir.dir().clear_item(data.header.parent_index).await?;
				}
				Ok(())
			}
			.await;
			if let Err(e) = r {
				mem::forget(dir);
				return Err(e);
			}

			dir.drop().await?;
		}
		Ok(())
	}

	impl_common!(s -> s, Ty::File);
}

impl<'a, 'b, D: Dev> SymRef<'a, 'b, D> {
	/// Create a new [`SymRef`] to an object.
	pub(crate) fn from_obj(dir: &Dir<'a, 'b, D>, id: u64, index: u32) -> Self {
		Self(FileRef::from_obj(dir, id, index))
	}

	/// Create a new [`SymRef`] to embedded data.
	pub(crate) fn from_embed(dir: &Dir<'a, 'b, D>, offset: u64, length: u16, index: u32) -> Self {
		Self(FileRef::from_embed(dir, offset, length, index))
	}

	/// Destroy the reference to this symbolic link.
	///
	/// This will perform cleanup if the symbolic link is dangling
	/// and this was the last reference.
	pub async fn drop(self) -> Result<(), Error<D>> {
		self.0.drop().await
	}

	impl_common!(s -> s.0, Ty::Sym);
}

impl<'a, 'b, D: Dev> UnknownRef<'a, 'b, D> {
	/// Create a new [`UnknownRef`].
	pub(crate) fn new(dir: &Dir<'a, 'b, D>, index: u32) -> Self {
		Self(FileRef::from_embed(dir, 0, 0, index))
	}

	/// Destroy the reference to this item.
	///
	/// This will perform cleanup if the item is dangling
	/// and this was the last reference.
	pub async fn drop(self) -> Result<(), Error<D>> {
		self.0.drop().await
	}

	// Do *not* use impl_common!
	// The actual type of UnknownRef is unknown.
	// Wrapping FileRef happes to be the most convenient and results in the least amount of
	// extra code.
}
