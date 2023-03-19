use {
	crate::{ext, Dev, Dir, DirKey, EnableExt, Error, Ext, FileKey, Name, Nrfs, TransferError},
	std::borrow::Cow,
};

#[derive(Debug)]
pub struct ItemInfo<'n> {
	pub(crate) dir: u64,
	pub(crate) index: u32,
	pub name: Option<Cow<'n, Name>>,
	pub(crate) data: ItemData,
	pub ext: ItemExt,
}

impl<'n> ItemInfo<'n> {
	/// Get a key to access this item.
	pub fn key(&self) -> ItemKey {
		match self.data.ty {
			ItemData::TY_DIR => ItemKey::Dir(DirKey {
				id: self.data.id_or_offset,
				dir: self.dir,
				index: self.index,
			}),
			ItemData::TY_FILE | ItemData::TY_EMBED_FILE => {
				ItemKey::File(FileKey::new(self.dir, self.index))
			}
			ItemData::TY_SYM | ItemData::TY_EMBED_SYM => {
				ItemKey::Sym(FileKey::new(self.dir, self.index))
			}
			ty => todo!("invalid ty {}", ty),
		}
	}
}

#[derive(Debug)]
pub struct Item<'a, D: Dev> {
	fs: &'a Nrfs<D>,
	key: ItemKey,
}

impl<'a, D: Dev> Item<'a, D> {
	/// Transfer this item to another directory.
	pub async fn transfer(
		&mut self,
		to_dir: &Dir<'a, D>,
		to_name: &Name,
	) -> Result<Result<ItemKey, TransferError>, Error<D>> {
		let res = match &mut self.key {
			ItemKey::Dir(d) => self
				.fs
				.dir(*d)
				.transfer(to_dir, to_name)
				.await?
				.map(|key| *d = key),
			ItemKey::File(f) => self
				.fs
				.file(*f)
				.transfer(to_dir, to_name)
				.await?
				.map(|key| *f = key),
			ItemKey::Sym(f) => self
				.fs
				.file(*f)
				.transfer(to_dir, to_name)
				.await?
				.map(|key| *f = key),
		};
		Ok(res.map(|()| self.key))
	}

	/// Erase the name of this item.
	pub async fn erase_name(&self) -> Result<(), Error<D>> {
		let (dir, index) = self.get_loc();
		if dir == u64::MAX {
			return Ok(());
		}
		let dir = self.fs.dir(DirKey::inval(dir));
		let mut hdr = dir.header().await?;
		dir.item_erase_name(&mut hdr, index).await?;
		dir.set_header(hdr).await
	}

	pub async fn ext(&self) -> Result<ItemExt, Error<D>> {
		let (dir, index) = self.get_loc();
		if dir == u64::MAX {
			let map = self.fs.ext.borrow_mut();
			let data = &self.fs.storage.header_data()[16..];
			Ok(ItemExt {
				unix: map
					.get_id(Ext::Unix)
					.map(|(_, offt)| ext::Unix::from_raw(data[offt..offt + 8].try_into().unwrap())),
				mtime: map.get_id(Ext::MTime).map(|(_, offt)| {
					ext::MTime::from_raw(data[offt..offt + 8].try_into().unwrap())
				}),
			})
		} else {
			let dir = Dir::new(self.fs, DirKey::inval(dir));
			let hdr = dir.header().await?;
			Ok(dir.item_get_data_ext(&hdr, index).await?.1)
		}
	}

	pub async fn set_unix(&self, unix: ext::Unix) -> Result<bool, Error<D>> {
		trace!("set_unix {:?}", self.key);
		assert!(!self.fs.read_only, "read only");
		let (dir, index) = self.get_loc();
		if dir == u64::MAX {
			let Some((_, offt)) = self.fs.ext.borrow_mut().get_id(Ext::Unix) else { return Ok(false) };
			self.fs.storage.header_data()[16 + offt..16 + offt + 8]
				.copy_from_slice(&unix.into_raw());
			Ok(true)
		} else {
			Dir::new(self.fs, DirKey::inval(dir))
				.item_set_unix(index, unix)
				.await
		}
	}

	pub async fn set_mtime(&self, mtime: ext::MTime) -> Result<bool, Error<D>> {
		trace!("set_unix {:?}", self.key);
		assert!(!self.fs.read_only, "read only");
		let (dir, index) = self.get_loc();
		if dir == u64::MAX {
			let Some((_, offt)) = self.fs.ext.borrow_mut().get_id(Ext::MTime) else { return Ok(false) };
			self.fs.storage.header_data()[16 + offt..16 + offt + 8]
				.copy_from_slice(&mtime.into_raw());
			Ok(true)
		} else {
			Dir::new(self.fs, DirKey::inval(dir))
				.item_set_mtime(index, mtime)
				.await
		}
	}

	pub fn key(&self) -> ItemKey {
		self.key
	}

	pub fn into_key(self) -> ItemKey {
		self.key
	}

	fn get_loc(&self) -> (u64, u32) {
		match self.key {
			ItemKey::Dir(d) => (d.dir, d.index),
			ItemKey::File(f) | ItemKey::Sym(f) => (f.dir(), f.index),
		}
	}
}

impl<D: Dev> Nrfs<D> {
	pub fn item(&self, key: ItemKey) -> Item<'_, D> {
		Item { fs: self, key }
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ItemKey {
	Dir(DirKey),
	File(FileKey),
	Sym(FileKey),
}

macro_rules! item_as {
	($as_f:ident $into_f:ident $as_v:ident $as_ty:ident) => {
		pub fn $as_f(&self) -> Option<&$as_ty> {
			let Self::$as_v(v) = self else { return None };
			Some(v)
		}

		pub fn $into_f(self) -> Option<$as_ty> {
			let Self::$as_v(v) = self else { return None };
			Some(v)
		}
	};
}

impl ItemKey {
	item_as!(as_dir into_dir Dir DirKey);
	item_as!(as_file into_file File FileKey);
	item_as!(as_sym into_sym Sym FileKey);
}

#[derive(Debug)]
pub(crate) struct ItemData {
	pub ty: u8,
	pub id_or_offset: u64,
	pub len: u64,
}

impl ItemData {
	pub const TY_NONE: u8 = 0;
	pub const TY_DIR: u8 = 1;
	pub const TY_FILE: u8 = 2;
	pub const TY_SYM: u8 = 3;
	pub const TY_EMBED_FILE: u8 = 4;
	pub const TY_EMBED_SYM: u8 = 5;
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(any(test, fuzzing), derive(arbitrary::Arbitrary))]
pub struct ItemExt {
	/// `unix` extension data.
	pub unix: Option<ext::Unix>,
	/// `mtime` extension data.
	pub mtime: Option<ext::MTime>,
}

#[derive(Debug)]
pub(crate) struct NewItem<'a> {
	pub name: &'a Name,
	pub ty: ItemTy,
	pub ext: ItemExt,
}

#[derive(Debug)]
pub(crate) enum ItemTy {
	Dir { ext: EnableExt },
	File,
	Sym,
}

impl<'a> NewItem<'a> {
	pub fn dir(name: impl Into<&'a Name>, enable_ext: EnableExt, ext: ItemExt) -> Self {
		Self { name: name.into(), ty: ItemTy::Dir { ext: enable_ext }, ext }
	}

	pub fn file(name: impl Into<&'a Name>, ext: ItemExt) -> Self {
		Self { name: name.into(), ty: ItemTy::File, ext }
	}

	pub fn sym(name: impl Into<&'a Name>, ext: ItemExt) -> Self {
		Self { name: name.into(), ty: ItemTy::Sym, ext }
	}
}
