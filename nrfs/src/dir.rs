use {
	crate::{Dev, Error, File, Item, ItemInfo, ItemKey, ItemTy, Nrfs, Store},
	core::{cell::RefCell, fmt, future::Future, ops::Deref},
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

type Conf = nrkv::StaticConf<0, 24>;
pub(crate) type Kv<'a, D> = nrkv::Nrkv<Store<'a, D>, Conf>;

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
		nrkv::Nrkv::init_with_key(Store(dir), Conf::CONF, key).await?;
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

		let mut kv = self.kv().await?;
		let Ok(tag) = kv.insert(key, &[]).await? else {
			return Ok(Err(CreateError::Duplicate))
		};
		kv.save().await?;

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
		let mut kv = self.kv().await?;
		let Some(tag) = kv.find(name).await? else { return Ok(None) };
		let ty = &mut [0];
		kv.read_user_data(tag, 0, ty).await?;
		Ok(Some(ItemInfo {
			key: ItemKey { dir: self.id, tag },
			name: Some(Cow::Borrowed(name)),
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

		let mut kv = self.kv().await?;
		let buf = &mut [0; 24];
		kv.read_user_data(key.tag, 0, buf).await?;
		let a = u64::from_le_bytes(buf[..8].try_into().unwrap());
		let b = u64::from_le_bytes(buf[8..16].try_into().unwrap());
		match a & 7 {
			ty @ 1 | ty @ 2 | ty @ 3 => {
				if ty == 1 && b != 0 {
					return Ok(Err(RemoveError::NotEmpty));
				}
				self.fs.get(a >> 5).dealloc().await?;
			}
			4 | 5 => {
				let (len, offt) = (b & 0xffff, b >> 16);
				kv.dealloc(offt, len.into()).await?;
			}
			ty => panic!("invalid ty {}", ty),
		}
		let (offt, len) = meta(buf[16..].try_into().unwrap());
		kv.dealloc(offt, len.into()).await?;
		kv.remove(key.tag).await?;
		kv.save().await?;
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

		let to_kv = &mut to_dir.kv().await?;

		if to_kv.find(to_name).await?.is_some() {
			return Ok(Err(TransferError::Duplicate));
		}

		let mut from_kv = if self.id != to_dir.id {
			Some(self.kv().await?)
		} else {
			None
		};

		let item = &mut [0; 24];
		let kv = from_kv.as_mut().unwrap_or(to_kv);
		kv.read_user_data(key.tag, 0, item).await?;
		kv.remove(key.tag).await?;

		assert_ne!(key, to_dir.key, "transfer causes cycle");

		if let Some(from_kv) = from_kv.as_mut() {
			let mut buf = vec![];
			if matches!(item[0] & 7, 4 | 5) {
				let offt = u64::from_le_bytes(item[..8].try_into().unwrap()) >> 16;
				let len = u16::from_le_bytes(item[8..10].try_into().unwrap());
				buf.resize(len.into(), 0);
				from_kv.read(offt, &mut buf).await?;
				from_kv.dealloc(offt, len.into()).await?;
				let offt = to_kv.alloc(len.into()).await?;
				to_kv.write(offt.get(), &buf).await?;
				item[2..8].copy_from_slice(&offt.get().to_le_bytes()[..6]);
				item[12..14].copy_from_slice(&len.to_le_bytes());
			}
			let (offt, len) = meta(item[16..].try_into().unwrap());
			buf.resize(len.into(), 0);
			from_kv.read(offt, &mut buf).await?;
			from_kv.dealloc(offt, len.into()).await?;
			let offt = to_kv.alloc(len.into()).await?;
			to_kv.write(offt.get(), &buf).await?;
			item[16..18].copy_from_slice(&len.to_le_bytes());
			item[18..].copy_from_slice(&offt.get().to_le_bytes()[..6]);
		}

		let tag = to_kv.insert(to_name, item).await?.unwrap();

		to_kv.save().await?;
		if let Some(mut from_kv) = from_kv {
			from_kv.save().await?;
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
		let val = &RefCell::new(None);
		let kv = &mut self.kv().await?;
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
				name: Some(Cow::Owned(
					Box::<Key>::try_from(key.into_boxed_slice()).unwrap(),
				)),
				key: ItemKey { dir: self.id, tag },
				ty: ItemTy::from_raw(ty[0] & 7).unwrap(),
			}));
			Ok(false)
		})
		.await?;
		Ok(val.take().map(|v| (v, state.into_u64())))
	}

	pub fn key(&self) -> ItemKey {
		self.key
	}

	pub async fn len(&self) -> Result<u32, Error<D>> {
		trace!("Dir::len");
		let mut kv = Dir::new(self.fs, ItemKey::INVAL, self.key.dir).kv().await?;
		let buf = &mut [0; 4];
		kv.read_user_data(self.key.tag, 8, buf).await?;
		Ok(u32::from_le_bytes(*buf))
	}

	pub(crate) fn kv(&self) -> impl Future<Output = Result<Kv<'a, D>, Error<D>>> {
		nrkv::Nrkv::load(Store(self.fs.get(self.id)), nrkv::StaticConf)
	}

	async fn update_item_count(&self, incr: bool) -> Result<(), Error<D>> {
		if self.key.dir == u64::MAX {
			return Ok(());
		}
		let mut kv = Dir::new(self.fs, ItemKey::INVAL, self.key.dir).kv().await?;
		let buf = &mut [0; 4];
		kv.read_user_data(self.key.tag, 8, buf).await?;
		let mut num = u32::from_le_bytes(*buf);
		if incr {
			num += 1
		} else {
			num -= 1
		};
		kv.write_user_data(self.key.tag, 8, &num.to_le_bytes())
			.await
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

fn meta(data: &[u8; 8]) -> (u64, u16) {
	let a = u64::from_le_bytes(*data);
	(a >> 16, a as _)
}
