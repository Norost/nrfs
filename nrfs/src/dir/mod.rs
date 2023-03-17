mod header;
mod heap;
mod op;

use {
	crate::{
		item::{ItemData, ItemExt, ItemInfo, ItemTy, NewItem},
		Dev, EnableExt, Error, File, FileKey, Name, Nrfs,
	},
	core::fmt,
	header::DirExt,
	std::borrow::Cow,
};

/// Key to a directory.
///
/// Can be paired with a [`Nrfs`] object to create a [`Dir`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DirKey {
	/// ID of parent directory.
	pub(crate) dir: u64,
	/// Index to corresponding item *data* in parent directory.
	/// The key does *not* point to the name but the *data* of the item.
	pub(crate) index: u32,
	/// ID of this directory.
	pub(crate) id: u64,
}

impl DirKey {
	/// Create stub key.
	pub(crate) fn inval(id: u64) -> Self {
		Self { id, dir: 0xdeaddeaddeaddead, index: 0xbeefbeef }
	}
}

/// Helper structure for working with directories.
#[derive(Debug)]
pub struct Dir<'a, D: Dev> {
	/// The filesystem containing the directory's data.
	pub(crate) fs: &'a Nrfs<D>,
	/// Key of this directory.
	pub(crate) key: DirKey,
}

impl<'a, D: Dev> Dir<'a, D> {
	/// Create a [`Dir`] helper structure.
	pub fn new(fs: &'a Nrfs<D>, key: DirKey) -> Self {
		Self { fs, key }
	}

	/// Get a list of enabled extensions.
	///
	/// This only includes extensions that are understood by this library.
	pub async fn enabled_ext(&self) -> Result<EnableExt, Error<D>> {
		let mut ext = EnableExt::default();
		let hdr = self.header().await?;
		for e in hdr.ext() {
			match e {
				DirExt::Unix { .. } => ext.add_unix(),
				DirExt::MTime { .. } => ext.add_mtime(),
				DirExt::Unknown { .. } => todo!(),
			};
		}
		Ok(ext)
	}

	/// Create a new item.
	///
	/// This fails if an item with the given name already exists.
	///
	/// # Returns
	///
	/// The index of the item as well as an ID if a directory has been created.
	async fn create(&self, item: NewItem<'_>) -> Result<Result<(u32, u64), CreateError>, Error<D>> {
		trace!("create {:?}", &item);
		assert!(!self.fs.read_only, "read only");

		let mut hdr = self.header().await?;

		let name_index = match self.find(&hdr, item.name).await? {
			op::FindResult::Found { .. } => return Ok(Err(CreateError::Duplicate)),
			op::FindResult::NotFound { data_index } => data_index,
		};

		let (ty, id_or_offset) = match item.ty {
			ItemTy::File => (ItemData::TY_EMBED_FILE, 0),
			ItemTy::Sym => (ItemData::TY_EMBED_SYM, 0),
			ItemTy::Dir { ext } => (ItemData::TY_DIR, Dir::init(self.fs, ext).await?),
		};

		let name = item.name;
		let item = DirNewItem { name, data: ItemData { ty, id_or_offset, len: 0 }, ext: item.ext };
		self.item_insert(&mut hdr, name_index, item).await?;
		self.set_header(hdr).await?;

		let index = name_index + u32::from(name_blocks(name));
		Ok(Ok((index, id_or_offset)))
	}

	/// Create a new directory.
	///
	/// This fails if an item with the given name already exists.
	pub async fn create_dir(
		&self,
		name: &Name,
		enable_ext: EnableExt,
		ext: ItemExt,
	) -> Result<Result<Dir<'a, D>, CreateError>, Error<D>> {
		let res = self.create(NewItem::dir(name, enable_ext, ext)).await?;
		let res = res.map(|(index, id)| DirKey { dir: self.key.id, index, id });
		Ok(res.map(|key| Dir::new(self.fs, key)))
	}

	/// Create a new file.
	///
	/// This fails if an item with the given name already exists.
	pub async fn create_file(
		&self,
		name: &Name,
		ext: ItemExt,
	) -> Result<Result<File<'a, D>, CreateError>, Error<D>> {
		let res = self.create(NewItem::file(name, ext)).await?;
		let res = res.map(|(index, _)| FileKey::new(self.key.id, index));
		Ok(res.map(|key| File::new(self.fs, key)))
	}

	/// Create a new symlink.
	///
	/// This fails if an item with the given name already exists.
	pub async fn create_sym(
		&self,
		name: &Name,
		ext: ItemExt,
	) -> Result<Result<File<'a, D>, CreateError>, Error<D>> {
		let res = self.create(NewItem::sym(name, ext)).await?;
		let res = res.map(|(index, _)| FileKey::new(self.key.id, index));
		Ok(res.map(|key| File::new(self.fs, key)))
	}

	/// Search for an item by name.
	pub async fn search<'n>(&self, name: &'n Name) -> Result<Option<ItemInfo<'n>>, Error<D>> {
		trace!("search {:#x} {:?}", self.key.id, name);
		let hdr = self.header().await?;
		Ok(match self.find(&hdr, name).await? {
			op::FindResult::Found { data_index } => {
				let (data, ext) = self.item_get_data_ext(&hdr, data_index).await?;
				Some(ItemInfo {
					dir: self.key.id,
					index: data_index,
					name: Cow::Borrowed(name),
					data,
					ext,
				})
			}
			op::FindResult::NotFound { .. } => None,
		})
	}

	/// Retrieve the entry with an index equal or greater than `index`.
	///
	/// Returns an item and next index if any is found.
	pub async fn next_from(
		&self,
		mut index: u32,
	) -> Result<Option<(ItemInfo<'static>, u32)>, Error<D>> {
		trace!("next_from {:?}", index);
		let hdr = self.header().await?;
		while index < hdr.highest_block {
			let name;
			(name, index) = self.item_name(index).await?;
			let Some(name) = name else { continue };

			let (data, ext) = self.item_get_data_ext(&hdr, index).await?;
			let item = ItemInfo { dir: self.key.id, index, name: Cow::Owned(name), data, ext };

			index += 1 + u32::from(hdr.ext_slots());

			return Ok(Some((item, index)));
		}
		Ok(None)
	}

	pub async fn ext(&self) -> Result<ItemExt, Error<D>> {
		trace!("ext");
		let dir = Dir::new(self.fs, DirKey::inval(self.key.dir));
		let hdr = dir.header().await?;
		Ok(dir.item_get_data_ext(&hdr, self.key.index).await?.1)
	}

	/// Destroy this directory.
	///
	/// The directory must be empty.
	pub async fn destroy(self) -> Result<Result<(), DirDestroyError>, Error<D>> {
		trace!("destroy {:#x}", self.key.id);
		assert!(!self.fs.read_only, "read only");
		if self.key.dir == u64::MAX {
			return Ok(Err(DirDestroyError::IsRoot));
		}
		if self.header().await?.blocks_used != 0 {
			return Ok(Err(DirDestroyError::NotEmpty));
		}
		let dir = Dir::new(self.fs, DirKey::inval(self.key.dir));
		let mut hdr = dir.header().await?;
		dir.item_destroy(&mut hdr, self.key.index).await?;
		dir.set_header(hdr).await?;
		Ok(Ok(()))
	}

	/// Transfer this directory.
	///
	/// # Note
	///
	/// This does not check for cycles!
	pub async fn transfer(
		&mut self,
		to_dir: &Dir<'a, D>,
		to_name: &Name,
	) -> Result<Result<DirKey, TransferError>, Error<D>> {
		trace!(
			"transfer {:#x} -> {:#x} {:?}",
			self.key.id,
			to_dir.key.id,
			to_name
		);
		assert!(!self.fs.read_only, "read only");
		if self.key.dir == u64::MAX {
			return Ok(Err(TransferError::IsRoot));
		}
		let dir = Dir::new(self.fs, DirKey::inval(self.key.dir));
		let to_index = match dir.item_transfer(self.key.index, to_dir, to_name).await? {
			Ok(i) => i,
			Err(e) => return Ok(Err(e)),
		};
		self.key.dir = to_dir.key.id;
		self.key.index = to_index;
		Ok(Ok(self.key))
	}

	pub fn into_key(self) -> DirKey {
		self.key
	}
}

#[derive(Debug)]
pub(crate) struct DirNewItem<'a> {
	name: &'a Name,
	data: ItemData,
	ext: ItemExt,
}

fn name_blocks(name: &Name) -> u16 {
	(u16::from(name.len_u8()) + 14) / 15
}

fn index_to_offset(index: u32) -> u64 {
	128 + u64::from(index) * 16
}

/// An error that occured while trying to destroy a directory.
#[derive(Clone, Debug)]
pub enum DirDestroyError {
	/// The directory was not empty.
	NotEmpty,
	/// The directory is the root directory.
	IsRoot,
}

/// An error that occured while trying to insert an entry.
#[derive(Clone, Debug)]
pub enum CreateError {
	/// An entry with the same name already exists.
	Duplicate,
	/// The directory is full.
	Full,
}

/// An error that occured while trying to transfer an entry.
#[derive(Clone, Debug)]
pub enum TransferError {
	/// An entry with the same name already exists.
	Duplicate,
	/// The target directory is full.
	Full,
	/// The root directory cannot be transferred
	IsRoot,
}

impl fmt::Display for DirDestroyError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NotEmpty => "not empty",
			Self::IsRoot => "is root",
		}
		.fmt(f)
	}
}

impl fmt::Display for CreateError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Duplicate => "duplicate",
			Self::Full => "full",
		}
		.fmt(f)
	}
}

impl fmt::Display for TransferError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Duplicate => "duplicate",
			Self::Full => "full",
			Self::IsRoot => "is root",
		}
		.fmt(f)
	}
}

impl core::error::Error for DirDestroyError {}
impl core::error::Error for CreateError {}
impl core::error::Error for TransferError {}
