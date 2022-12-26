use {
	super::{ext, Dev, Dir, DirData, Key},
	crate::{DataHeader, DirRef, Error, FileRef, Name, Nrfs, SymRef, UnknownRef},
	core::cell::RefMut,
};

const TY_NONE: u8 = 0;
const TY_DIR: u8 = 1;
const TY_FILE: u8 = 2;
const TY_SYM: u8 = 3;
const TY_EMBED_FILE: u8 = 4;
const TY_EMBED_SYM: u8 = 5;

/// A single directory item.
#[derive(Debug)]
pub(crate) struct Item {
	/// The type of this item.
	///
	/// May be set to a value other than [`Type::None`] if the item is or was dangling,
	/// e.g. due to live references.
	pub ty: Type,
	/// Other item data.
	pub data: ItemData,
	/// The index of this item in the item list.
	pub index: u32,
}

#[derive(Debug)]
/// Other item data.
pub struct ItemData {
	/// The key of this item.
	pub(super) key: Option<Key>,
	/// `unix` extension data.
	pub ext_unix: Option<ext::unix::Entry>,
	/// `mtime` extension data.
	pub ext_mtime: Option<ext::mtime::Entry>,
}

impl Item {
	/// Convert [`Self`] to raw data.
	///
	/// # Panics
	///
	/// If the type is [`Type::Unknown`].
	/// Unknown types have unknown data and cannot be serialized.
	pub(super) fn to_raw(&self, dir: &DirData, buf: &mut [u8]) {
		// Set type
		buf[28..28 + 12].copy_from_slice(&self.ty.to_raw());

		// Set key
		if let Some(key) = &self.data.key {
			buf[..28].copy_from_slice(&key.to_raw());
		}

		// Set unix info
		dir.unix_offset
			.map(usize::from)
			.and_then(|o| self.data.ext_unix.map(|e| (o, e)))
			.map(|(o, e)| buf[o..o + 8].copy_from_slice(&e.into_raw()));

		// Get mtime info
		dir.mtime_offset
			.map(usize::from)
			.and_then(|o| self.data.ext_mtime.map(|e| (o, e)))
			.map(|(o, e)| buf[o..o + 8].copy_from_slice(&e.into_raw()));
	}

	/// Create an [`Item`] from raw data.
	pub(super) fn from_raw(dir: &DirData, data: &[u8], index: u32) -> Item {
		// Get type
		let ty = Type::from_raw(&data[28..28 + 12].try_into().unwrap());

		// Get key
		let key = Key::from_raw(&data[..28].try_into().unwrap());

		// Get unix info
		let ext_unix = dir
			.unix_offset
			.map(usize::from)
			.map(|o| ext::unix::Entry::from_raw(data[o..o + 8].try_into().unwrap()));

		// Get mtime info
		let ext_mtime = dir
			.mtime_offset
			.map(usize::from)
			.map(|o| ext::mtime::Entry::from_raw(data[o..o + 8].try_into().unwrap()));

		Item { ty, data: ItemData { key, ext_unix, ext_mtime }, index }
	}
}

/// The type of an item.
#[derive(Debug)]
pub(crate) enum Type {
	/// No type, i.e. invalid.
	None,
	/// Directory.
	Dir { id: u64 },
	/// File with data in an object.
	File { id: u64 },
	/// Symlink with data in an object.
	Sym { id: u64 },
	/// File with embedded data.
	EmbedFile { offset: u64, length: u16 },
	/// Symlink with embedded data.
	EmbedSym { offset: u64, length: u16 },
	/// Unrecognized type.
	Unknown(u8),
}

impl Type {
	/// Convert this type to raw data for storage.
	pub(super) fn to_raw(&self) -> [u8; 12] {
		let mut buf = [0; 12];

		// Set type
		buf[0] = match self {
			Self::None => TY_NONE,
			Self::Dir { .. } => TY_DIR,
			Self::File { .. } => TY_FILE,
			Self::Sym { .. } => TY_SYM,
			Self::EmbedFile { .. } => TY_EMBED_FILE,
			Self::EmbedSym { .. } => TY_EMBED_SYM,
			Self::Unknown(n) => panic!("unknown type {:?}", n),
		};

		// Set other data.
		match self {
			Self::None => {}
			Self::Dir { id } | Self::File { id } | Self::Sym { id } => {
				buf[4..].copy_from_slice(&id.to_le_bytes());
			}
			Self::EmbedFile { offset, length } | Self::EmbedSym { offset, length } => {
				buf[2..4].copy_from_slice(&length.to_le_bytes());
				buf[4..].copy_from_slice(&offset.to_le_bytes());
			}
			Self::Unknown(n) => panic!("unknown type {:?}", n),
		};

		buf
	}

	/// Create a [`Type`] from raw data.
	pub(super) fn from_raw(data: &[u8; 12]) -> Self {
		let &[ty, _, a, b, id_or_offset @ ..] = data;
		let length = u16::from_le_bytes([a, b]);
		let id @ offset = u64::from_le_bytes(id_or_offset);

		match ty {
			TY_NONE => Self::None,
			TY_DIR => Self::Dir { id },
			TY_FILE => Self::File { id },
			TY_SYM => Self::Sym { id },
			TY_EMBED_FILE => Self::EmbedFile { offset, length },
			TY_EMBED_SYM => Self::EmbedSym { offset, length },
			ty => Self::Unknown(ty),
		}
	}
}

/// A reference to an item.
#[must_use = "Must be manually dropped with ItemRef::drop"]
pub enum ItemRef<'a, D: Dev> {
	Dir(DirRef<'a, D>),
	File(FileRef<'a, D>),
	Sym(SymRef<'a, D>),
	Unknown(UnknownRef<'a, D>),
}

impl<'a, D: Dev> ItemRef<'a, D> {
	/// Construct an item from raw entry data and the corresponding directory.
	///
	/// # Panics
	///
	/// If the type is [`Type::None`].
	pub(super) async fn new(dir: &Dir<'a, D>, item: &Item) -> Result<ItemRef<'a, D>, Error<D>> {
		Ok(match item.ty {
			Type::None => panic!("can't reference none type"),
			Type::File { id } => Self::File(FileRef::from_obj(dir, id, item.index)),
			Type::Sym { id } => Self::Sym(SymRef::from_obj(dir, id, item.index)),
			Type::EmbedFile { offset, length } => {
				Self::File(FileRef::from_embed(dir, offset, length, item.index))
			}
			Type::EmbedSym { offset, length } => {
				Self::Sym(SymRef::from_embed(dir, offset, length, item.index))
			}
			Type::Dir { id } => Self::Dir(DirRef::load(dir, item.index, id).await?),
			Type::Unknown(_) => Self::Unknown(UnknownRef::new(dir, item.index)),
		})
	}

	/// Get item data, i.e. data in the entry itself, excluding heap data.
	pub async fn data(&self) -> Result<ItemData, Error<D>> {
		// Root dir doesn't have a parent, so it has no attributes.
		// TODO we should store attrs in filesystem header.
		if let Self::Dir(d) = self {
			if d.id == 0 {
				return Ok(ItemData { key: None, ext_unix: None, ext_mtime: None });
			}
		}

		let fs = self.fs();
		let DataHeader { parent_index, parent_id, .. } = *self.data_header();
		let item = Dir::new(fs, parent_id).get(parent_index).await?;
		Ok(item.data)
	}

	/// Get the key / name of this item.
	///
	/// `data` must be returned from [`data`].
	///
	/// # Note
	///
	/// May be [`None`] if the entry is dangling.
	pub async fn key(&self, data: &ItemData) -> Result<Option<Box<Name>>, Error<D>> {
		Ok(match data.key {
			None => None,
			Some(Key::Embed { len, data }) => {
				let data = <&Name>::try_from(&data[..len.get().into()]).expect("invalid name len");
				Some(Box::from(data))
			}
			Some(Key::Heap { len, offset, .. }) => {
				let mut buf = vec![0; len.get().into()];
				self.parent_dir().read_heap(offset, &mut buf).await?;
				Some(Box::<Name>::try_from(buf.into_boxed_slice()).expect("invalid name len"))
			}
		})
	}

	/// Set `unix` extension data.
	///
	/// Returns `false` if the extension is not enabled for the parent directory.
	pub async fn set_ext_unix(&self, data: &ext::unix::Entry) -> Result<bool, Error<D>> {
		trace!("set_ext_unix {:?}", data);
		// Root dir has no attributes.
		if matches!(self, Self::Dir(d) if d.id == 0) {
			return Ok(false);
		}
		let index = self.data_header().parent_index;
		self.parent_dir().ext_set_unix(index, data).await
	}

	/// Set `mtime` extension data.
	///
	/// Returns `false` if the extension is not enabled for the parent directory.
	pub async fn set_ext_mtime(&self, data: &ext::mtime::Entry) -> Result<bool, Error<D>> {
		trace!("set_ext_mtime {:?}", data);
		// Root dir has no attributes.
		if matches!(self, Self::Dir(d) if d.id == 0) {
			return Ok(false);
		}
		let index = self.data_header().parent_index;
		self.parent_dir().ext_set_mtime(index, data).await
	}

	/// Get a reference to the parent.
	///
	/// May be `None` if this item is the parent directory.
	pub fn parent(&self) -> Option<DirRef<'a, D>> {
		if matches!(self, Self::Dir(d) if d.id == 0) {
			return None;
		}
		let id = self.data_header().parent_id;
		let fs = self.fs();
		fs.dir_data(id).header.reference_count += 1;
		Some(DirRef { fs, id })
	}

	/// Get a reference to the filesystem containing this item's data.
	fn fs(&self) -> &'a Nrfs<D> {
		match self {
			Self::Dir(e) => e.fs,
			Self::File(e) => e.fs,
			Self::Sym(e) => e.0.fs,
			Self::Unknown(e) => e.0.fs,
		}
	}

	/// Get a reference to the [`DataHeader`] of this item.
	fn data_header(&self) -> RefMut<'a, DataHeader> {
		match self {
			Self::Dir(e) => RefMut::map(e.fs.dir_data(e.id), |d| &mut d.header),
			Self::File(e) => RefMut::map(e.fs.file_data(e.idx), |d| &mut d.header),
			Self::Sym(e) => RefMut::map(e.0.fs.file_data(e.0.idx), |d| &mut d.header),
			Self::Unknown(e) => RefMut::map(e.0.fs.file_data(e.0.idx), |d| &mut d.header),
		}
	}

	/// Create a parent dir helper.
	fn parent_dir(&self) -> Dir<'a, D> {
		Dir::new(self.fs(), self.data_header().parent_id)
	}

	/// Destroy the reference to this item.
	///
	/// This will perform cleanup if the item is dangling
	/// and this was the last reference.
	pub async fn drop(self) -> Result<(), Error<D>> {
		match self {
			Self::Dir(e) => e.drop().await,
			Self::File(e) => e.drop().await,
			Self::Sym(e) => e.drop().await,
			Self::Unknown(e) => e.drop().await,
		}
	}
}
