use crate::HDR_ROOT_OFFT;

use {
	crate::{item::ITEM_LEN, Dev, Error, File, Item, ItemInfo, ItemKey, ItemTy, Nrfs, Store},
	core::{cell::RefCell, fmt, ops::Deref},
	nrkv::Key,
	nros::Resource,
	std::borrow::Cow,
};

/// Helper structure for working with directories.
#[derive(Debug)]
pub struct Dir<'a, D: Dev> {
	pub(crate) item: Item<'a, D>,
	pub(crate) id: u64,
}

pub(crate) type Kv<'a, D> = nrkv::Nrkv<Store<'a, D>, nrkv::StaticConf<0, ITEM_LEN>>;

impl<'a, D: Dev> Dir<'a, D> {
	/// Create a [`Dir`] helper structure.
	pub fn new(fs: &'a Nrfs<D>, key: ItemKey, id: u64) -> Self {
		Self { item: Item::new(fs, key), id }
	}

	/// Create a new directory.
	pub(crate) async fn init(fs: &'a Nrfs<D>) -> Result<u64, Error<D>> {
		trace!("Dir::init");
		let dir = fs.storage.create().await?;
		let id = dir.id();
		trace!("--> {:#x}", id);
		let mut key = [0; 16];
		fs.resource().crng_fill(&mut key);
		Kv::init_with_key(Store { fs, id }, nrkv::StaticConf, key).await?;
		Ok(id)
	}

	/// Create a new item.
	///
	/// This fails if an item with the given name already exists.
	///
	/// # Returns
	///
	/// The index of the item as well as an ID if a directory has been created.
	async fn create(
		&self,
		key: &Key,
	) -> Result<Result<(ItemKey, Kv<'a, D>), CreateError>, Error<D>> {
		trace!("create {:#x} {:?}", self.id, key);
		assert!(!self.fs.read_only, "read only");

		let lock = self.fs.lock_dir_mut(self.id).await;
		let mut kv = self.kv();
		let Ok(tag) = kv.insert(key, &[]).await? else {
			return Ok(Err(CreateError::Duplicate))
		};
		drop(lock);

		self.update_item_count(true).await?;
		Ok(Ok((ItemKey { dir: self.id, tag }, kv)))
	}

	/// Create a new directory.
	///
	/// This fails if an item with the given name already exists.
	pub async fn create_dir(
		&self,
		name: &Key,
	) -> Result<Result<Dir<'a, D>, CreateError>, Error<D>> {
		match self.create(name).await? {
			Err(e) => Ok(Err(e)),
			Ok((key, mut kv)) => {
				let id = Dir::init(self.fs).await?;
				kv.write_user_data(key.tag, 0, &(id << 5 | 1).to_le_bytes())
					.await?;
				Ok(Ok(Dir::new(self.fs, key, id)))
			}
		}
	}

	/// Create a new file.
	///
	/// This fails if an item with the given name already exists.
	pub async fn create_file(
		&self,
		name: &Key,
	) -> Result<Result<File<'a, D>, CreateError>, Error<D>> {
		match self.create(name).await? {
			Err(e) => Ok(Err(e)),
			Ok((key, mut kv)) => {
				kv.write_user_data(key.tag, 0, &[4]).await?;
				Ok(Ok(self.fs.file(key)))
			}
		}
	}

	/// Create a new symlink.
	///
	/// This fails if an item with the given name already exists.
	pub async fn create_sym(
		&self,
		name: &Key,
	) -> Result<Result<File<'a, D>, CreateError>, Error<D>> {
		match self.create(name).await? {
			Err(e) => Ok(Err(e)),
			Ok((key, mut kv)) => {
				kv.write_user_data(key.tag, 0, &[5]).await?;
				Ok(Ok(self.fs.file(key)))
			}
		}
	}

	/// Search for an item by name.
	pub async fn search<'n>(&self, name: &'n Key) -> Result<Option<ItemInfo<'n>>, Error<D>> {
		trace!("search {:#x} {:?}", self.id, name);
		let _lock = self.fs.lock_dir(self.id).await;
		let mut kv = self.kv();
		let Some(tag) = kv.find(name).await? else { return Ok(None) };
		let ty = &mut [0];
		kv.read_user_data(tag, 0, ty).await?;
		Ok(Some(ItemInfo {
			key: ItemKey { dir: self.id, tag },
			name: Cow::Borrowed(name),
			ty: ItemTy::from_raw(ty[0] & 7).unwrap(),
		}))
	}

	/// Remove an item.
	///
	/// # Panics
	///
	/// If the dir of the key does not match the ID of this directory.
	///
	/// # Warning
	///
	/// The key may not be reused if this call succeeds.
	pub async fn remove(&self, key: ItemKey) -> Result<Result<(), RemoveError>, Error<D>> {
		trace!("remove {:?}", key);
		assert_eq!(key.dir, self.id, "dir mismatch");

		let lock = self.fs.lock_dir_mut(self.id).await;
		if !Item::new(self.fs, key).destroy().await? {
			return Ok(Err(RemoveError::NotEmpty));
		}
		self.kv().remove(key.tag).await?;
		drop(lock);

		self.update_item_count(false).await?;
		Ok(Ok(()))
	}

	/// Move an entry to another directory.
	///
	/// # Panics
	///
	/// If the dir of the key does not match the ID of this directory.
	///
	/// # Warning
	///
	/// This does not check for cycles!
	pub async fn transfer(
		&self,
		key: ItemKey,
		to_dir: &Dir<'a, D>,
		to_name: &Key,
	) -> Result<Result<ItemKey, TransferError>, Error<D>> {
		trace!("transfer {:?} -> {:#x} {:?}", key, to_dir.id, to_name);
		assert_ne!(key, to_dir.key, "transfer causes cycle");

		let id_l = self.id.min(to_dir.id);
		let id_h = self.id.max(to_dir.id);
		let _lock_l = self.fs.lock_dir_mut(id_l).await;
		let _lock_h = if id_l != id_h {
			Some(self.fs.lock_dir_mut(id_h).await)
		} else {
			None
		};

		if to_dir.kv().find(to_name).await?.is_some() {
			return Ok(Err(TransferError::Duplicate));
		}

		let item = &mut [0; ITEM_LEN as _];
		self.kv().read_user_data(key.tag, 0, item).await?;
		self.kv().remove(key.tag).await?;

		self.fs.item(key).realloc(to_dir, item).await?;

		let tag = to_dir.kv().insert(to_name, item).await?.unwrap();

		if self.id != to_dir.id {
			self.update_item_count(false).await?;
			to_dir.update_item_count(true).await?;
		}

		Ok(Ok(ItemKey { dir: to_dir.id, tag }))
	}

	/// Retrieve the entry with an index equal or greater than `index`.
	///
	/// Returns an item and next index if any is found.
	pub async fn next_from(
		&self,
		state: u64,
	) -> Result<Option<(ItemInfo<'static>, u64)>, Error<D>> {
		trace!("next_from {:#x}", state);
		let _lock = self.fs.lock_dir(self.id).await;

		let val = &RefCell::new(None);
		let kv = &mut self.kv();
		let kv = &nrkv::ShareNrkv::new(kv);
		let mut state = nrkv::IterState::from_u64(state);
		kv.next_batch(&mut state, move |tag| async move {
			let len = kv.borrow_mut().read_key(tag, &mut []).await?;
			if len == 0 {
				return Ok(true);
			}
			let mut key = vec![0; len.into()];
			kv.borrow_mut().read_key(tag, &mut key).await?;
			let ty = &mut [0];
			kv.borrow_mut().read_user_data(tag, 0, ty).await?;
			val.replace(Some(ItemInfo {
				name: Cow::Owned(Box::<Key>::try_from(key.into_boxed_slice()).unwrap()),
				key: ItemKey { dir: self.id, tag },
				ty: ItemTy::from_raw(ty[0] & 7).unwrap(),
			}));
			Ok(false)
		})
		.await?;
		Ok(val.take().map(|v| (v, state.into_u64())))
	}

	pub(crate) fn kv(&self) -> Kv<'a, D> {
		nrkv::Nrkv::wrap(Store { fs: self.fs, id: self.id }, nrkv::StaticConf)
	}

	async fn update_item_count(&self, incr: bool) -> Result<(), Error<D>> {
		let f = |buf| {
			let mut num = u32::from_le_bytes(buf);
			if incr {
				num += 1
			} else {
				num -= 1
			};
			num.to_le_bytes()
		};
		if self.key.dir == u64::MAX {
			let b = &mut self.fs.storage.header_data_mut()[HDR_ROOT_OFFT..][8..12];
			let b = <&mut [u8; 4]>::try_from(b).unwrap();
			*b = f(*b);
			Ok(())
		} else {
			let _lock = self.fs.lock_item_mut(self.key).await;
			let mut kv = Dir::new(self.fs, ItemKey::INVAL, self.key.dir).kv();
			let buf = &mut [0; 4];
			kv.read_user_data(self.key.tag, 8, buf).await?;
			kv.write_user_data(self.key.tag, 8, &f(*buf)).await
		}
	}
}

impl<'a, D: Dev> Deref for Dir<'a, D> {
	type Target = Item<'a, D>;

	fn deref(&self) -> &Self::Target {
		&self.item
	}
}

/// An error that occured while trying to insert an entry.
#[derive(Clone, Debug)]
pub enum CreateError {
	/// An entry with the same name already exists.
	Duplicate,
	/// The directory is full.
	Full,
}

#[derive(Clone, Debug)]
pub enum RemoveError {
	NotEmpty,
}

/// An error that occured while trying to transfer an entry.
#[derive(Clone, Debug)]
pub enum TransferError {
	/// An entry with the same name already exists.
	Duplicate,
	/// The target directory is full.
	Full,
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

impl fmt::Display for RemoveError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NotEmpty => "not empty",
		}
		.fmt(f)
	}
}

impl fmt::Display for TransferError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Duplicate => "duplicate",
			Self::Full => "full",
		}
		.fmt(f)
	}
}

impl core::error::Error for CreateError {}
impl core::error::Error for RemoveError {}
impl core::error::Error for TransferError {}
