use {
	super::{ext, Dev, Dir, DirData, Index, Name, Offset},
	crate::{DataHeader, DirRef, Error, FileRef, Nrfs, ObjectId, SymRef, UnknownRef},
	core::cell::RefMut,
};

pub(super) const OFFT_NAME: u16 = 0;
pub(super) const OFFT_DATA: u16 = 16;
pub(super) const OFFT_META: u16 = 32;

const TY_NONE: u8 = 0;
const TY_DIR: u8 = 1;
const TY_FILE: u8 = 2;
const TY_SYM: u8 = 3;
const TY_EMBED_FILE: u8 = 4;
const TY_EMBED_SYM: u8 = 5;

#[derive(Debug)]
/// Other item data.
pub struct ItemData {
	/// The name of this item.
	///
	/// `None` if dangling.
	pub(super) name: Option<Box<Name>>,
	/// `unix` extension data.
	pub ext_unix: Option<ext::unix::Entry>,
	/// `mtime` extension data.
	pub ext_mtime: Option<ext::mtime::Entry>,
}

/// The type of an item.
#[derive(Debug)]
pub(crate) enum Type {
	/// No type, i.e. invalid.
	None,
	/// Directory.
	Dir { id: ObjectId, item_count: [u8; 3] },
	/// File with data in an object.
	File { id: ObjectId, length: u64 },
	/// Symlink with data in an object.
	Sym { id: ObjectId, length: u64 },
	/// File with embedded data.
	EmbedFile { offset: Offset, length: u16 },
	/// Symlink with embedded data.
	EmbedSym { offset: Offset, length: u16 },
	/// Unrecognized type.
	Unknown(u8),
}

impl Type {
	/// Convert this type to raw data for storage.
	pub(super) fn to_raw(&self) -> [u8; 16] {
		let mut buf = [0; 16];

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
			Self::Dir { id, item_count } => {
				buf[1..8].copy_from_slice(&id.to_raw());
				buf[8..11].copy_from_slice(item_count);
			}
			Self::File { id, length } | Self::Sym { id, length } => {
				buf[1..8].copy_from_slice(&id.to_raw());
				buf[8..].copy_from_slice(&length.to_le_bytes());
			}
			Self::EmbedFile { offset, length } | Self::EmbedSym { offset, length } => {
				buf[2..8].copy_from_slice(&offset.to_raw());
				buf[8..10].copy_from_slice(&length.to_le_bytes());
			}
			Self::Unknown(n) => panic!("unknown type {:?}", n),
		};

		buf
	}

	/// Create a [`Type`] from raw data.
	pub(super) fn from_raw(data: &[u8; 16]) -> Self {
		let id = ObjectId::from_raw(data[1..8].try_into().unwrap());
		let offset = Offset::from_raw(data[2..8].try_into().unwrap());
		let len64 = u64::from_le_bytes(data[8..].try_into().unwrap());
		let len16 = len64 as u16;
		let item_count = data[8..11].try_into().unwrap();

		match data[0] {
			TY_NONE => Self::None,
			TY_DIR => Self::Dir { id, item_count },
			TY_FILE => Self::File { id, length: len64 },
			TY_SYM => Self::Sym { id, length: len64 },
			TY_EMBED_FILE => Self::EmbedFile { offset, length: len16 },
			TY_EMBED_SYM => Self::EmbedSym { offset, length: len16 },
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
	/// Construct an item.
	///
	/// # Panics
	///
	/// If the type is [`Type::None`].
	pub(super) async fn new(
		dir: &Dir<'a, D>,
		ty: Type,
		index: Index,
	) -> Result<ItemRef<'a, D>, Error<D>> {
		Ok(match ty {
			Type::None => panic!("can't reference none type"),
			Type::File { id, length } => Self::File(FileRef::from_obj(dir, id, length, index)),
			Type::Sym { id, length } => Self::Sym(SymRef::from_obj(dir, id, length, index)),
			Type::EmbedFile { offset, length } => {
				Self::File(FileRef::from_embed(dir, offset, length, index))
			}
			Type::EmbedSym { offset, length } => {
				Self::Sym(SymRef::from_embed(dir, offset, length, index))
			}
			Type::Dir { id, item_count } => Self::Dir(DirRef::load(dir, index, id).await?),
			Type::Unknown(_) => Self::Unknown(UnknownRef::new(dir, index)),
		})
	}

	/// Get item data, i.e. data in the entry itself, excluding heap data.
	pub async fn data(&self) -> Result<ItemData, Error<D>> {
		// Root dir doesn't have a parent, so it has no attributes.
		// TODO we should store attrs in filesystem header.
		if matches!(self, Self::Dir(d) if d.id == ObjectId::ROOT) {
			return Ok(ItemData { name: None, ext_unix: None, ext_mtime: None });
		}

		let fs = self.fs();
		let DataHeader { parent_index, parent_id, .. } = *self.data_header();
		let item = Dir::new(fs, parent_id).get(parent_index).await?;
		Ok(item.data)
	}

	/// Set `unix` extension data.
	///
	/// Returns `false` if the extension is not enabled for the parent directory.
	pub async fn set_ext_unix(&self, data: &ext::unix::Entry) -> Result<bool, Error<D>> {
		trace!("set_ext_unix {:?}", data);
		// Root dir has no attributes.
		if matches!(self, Self::Dir(d) if d.id == ObjectId::ROOT) {
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
		if matches!(self, Self::Dir(d) if d.id == ObjectId::ROOT) {
			return Ok(false);
		}
		let index = self.data_header().parent_index;
		self.parent_dir().ext_set_mtime(index, data).await
	}

	/// Get a reference to the parent.
	///
	/// May be `None` if this item is the parent directory.
	pub fn parent(&self) -> Option<DirRef<'a, D>> {
		if matches!(self, Self::Dir(d) if d.id == ObjectId::ROOT) {
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
