pub mod ext;

mod child;
mod dir_data;
mod heap;
mod index;
mod item;
mod object_id;
mod offset;
mod op;

pub use item::{ItemData, ItemRef};

pub(crate) use {
	child::Child, dir_data::DirData, index::Index, item::Type, object_id::ObjectId, offset::Offset,
};

use {
	crate::{DataHeader, Dev, DirRef, Error, FileRef, Name, Nrfs, SymRef, TmpRef},
	alloc::collections::btree_map,
	core::mem,
};

// TODO determine a good load factor.
const MAX_LOAD_FACTOR_MILLI: u64 = 875;
const MIN_LOAD_FACTOR_MILLI: u64 = 375;

/// Helper structure for working with directories.
#[derive(Debug)]
pub(crate) struct Dir<'a, D: Dev> {
	/// The filesystem containing the directory's data.
	pub(crate) fs: &'a Nrfs<D>,
	/// The ID of this directory.
	pub(crate) id: ObjectId,
}

impl<'a, D: Dev> Dir<'a, D> {
	/// Create a [`Dir`] helper structure.
	pub(crate) fn new(fs: &'a Nrfs<D>, id: ObjectId) -> Self {
		Self { fs, id }
	}

	/// Update the entry count.
	async fn update_item_count(&self, f: impl FnOnce(u32) -> u32) -> Result<(), Error<D>> {
		trace!("update_item_count");
		todo!()
	}

	/// Get an item.
	async fn get(&self, index: Index) -> Result<Item, Error<D>> {
		trace!("get {:?}", index);
		let d = self.fs.dir_data(self.id);
		let offt = u64::from(d.header_len()) + u64::from(d.item_size()) * u64::from(index);
		let item_len = d.item_size();
		drop(d);
		let mut buf = vec![0; item_len.into()];
		let obj = self.fs.storage.get(self.id).await?;
		crate::read_exact(&obj, offt, &mut buf).await?;
		let item = Item::from_raw(&self.fs.dir_data(self.id), &buf, index);
		Ok(item)
	}
}

impl<'a, D: Dev> DirRef<'a, D> {
	/// Create a new directory.
	pub(crate) async fn new(
		parent_dir: &Dir<'a, D>,
		parent_index: Index,
		options: &DirOptions,
	) -> Result<DirRef<'a, D>, Error<D>> {
		// Increase reference count to parent directory.
		let mut parent = parent_dir.fs.dir_data(parent_dir.id);
		parent.header.reference_count += 1;
		debug_assert!(
			!parent.children.contains_key(&parent_index),
			"child present in parent"
		);

		// Load directory data.
		drop(parent);
		let dir_ref = Self::new_inner(parent_dir.fs, parent_dir.id, parent_index, options).await?;

		let r = parent_dir
			.fs
			.dir_data(parent_dir.id)
			.children
			.insert(parent_index, Child::Dir(dir_ref.id));
		debug_assert!(r.is_none(), "child present in parent");

		Ok(dir_ref)
	}

	/// Create a new root directory.
	///
	/// This does not lock anything and is meant to be solely used in [`Nrfs::new`].
	pub(crate) async fn new_root(
		fs: &'a Nrfs<D>,
		options: &DirOptions,
	) -> Result<DirRef<'a, D>, Error<D>> {
		Self::new_inner(fs, u64::MAX, u32::MAX, options).await
	}

	/// Create a new directory.
	///
	/// This does not directly create a reference to a parent directory.
	async fn new_inner(
		fs: &'a Nrfs<D>,
		parent_id: ObjectId,
		parent_index: Index,
		options: &DirOptions,
	) -> Result<DirRef<'a, D>, Error<D>> {
		// Initialize data.
		let mut header_len = 32;
		let mut item_len = 32 + 8; // header + object id or data offset
		let unix_offset = options.extensions.unix().then(|| {
			header_len += 8; // 4, 2, "unix", offset
			let o = item_len;
			item_len += 8;
			o
		});
		let mtime_offset = options.extensions.mtime().then(|| {
			header_len += 9; // 5, 2, "mtime", offset
			let o = item_len;
			item_len += 8;
			o
		});
		let data = DirData {
			header: DataHeader::new(parent_id, parent_index),
			children: Default::default(),
			header_len8: ((header_len + 7) / 8).try_into().unwrap(),
			item_len8: ((item_len + 7) / 8).try_into().unwrap(),
			hashmap_size: DirSize::B1,
			hasher: options.hasher,
			item_count: 0,
			item_capacity: 0,
			unix_offset,
			mtime_offset,
			heap_alloc_map: Some(Default::default()),
			item_alloc_map: Some(Default::default()),
			is_dangling: false,
		};

		// Create objects (dir, map, heap).
		let slf_id = fs.storage.create_many(3).await?;

		// Create header.
		let (hash_ty, hash_key) = data.hasher.to_raw();

		let mut buf = [0; 64];
		buf[0] = data.header_len8;
		buf[1] = data.item_len8;
		buf[2] = hash_ty;
		buf[3] = data.hashmap_size.to_raw();
		buf[4..8].copy_from_slice(&data.item_count.to_le_bytes());
		buf[8..12].copy_from_slice(&data.item_capacity.to_le_bytes());
		buf[16..32].copy_from_slice(&hash_key);
		let mut header_offt = 32;

		let buf = &mut buf[..usize::from(data.header_len8) * 8];
		if let Some(offt) = data.unix_offset {
			buf[header_offt + 0] = 4; // name len
			buf[header_offt + 1] = 2; // data len
			buf[header_offt + 2..][..4].copy_from_slice(b"unix");
			buf[header_offt + 6..][..2].copy_from_slice(&offt.to_le_bytes());
			header_offt += 8;
		}
		if let Some(offt) = data.mtime_offset {
			buf[header_offt + 0] = 5; // name len
			buf[header_offt + 1] = 2; // data len
			buf[header_offt + 2..][..5].copy_from_slice(b"mtime");
			buf[header_offt + 7..][..2].copy_from_slice(&offt.to_le_bytes());
		}

		// Write header
		let header_len = usize::from(data.header_len8) * 8;
		fs.get(slf_id).await?.write(0, &buf[..header_len]).await?;

		// Insert directory data & return reference.
		fs.data.borrow_mut().directories.insert(slf_id, data);
		Ok(Self { fs, id: slf_id })
	}

	/// Load an existing directory.
	pub(crate) async fn load(
		parent_dir: &Dir<'a, D>,
		parent_index: Index,
		id: ObjectId,
	) -> Result<DirRef<'a, D>, Error<D>> {
		trace!("load {:?} | {:?}:{:?}", id, parent_dir.id, parent_index);
		// Check if the directory is already present in the filesystem object.
		//
		// If so, just reference that and return.
		if let Some(dir) = parent_dir.fs.data.borrow_mut().directories.get_mut(&id) {
			dir.header.reference_count += 1;
			return Ok(DirRef { fs: parent_dir.fs, id });
		}

		// FIXME check if the directory is already being loaded
		// Also create some guard when we start fetching directory data.

		// Increase reference count to parent directory.
		let mut parent = parent_dir.fs.dir_data(parent_dir.id);
		parent.header.reference_count += 1;
		debug_assert!(
			!parent.children.contains_key(&parent_index),
			"child present in parent"
		);

		// Load directory data.
		drop(parent);
		let dir_ref = Self::load_inner(parent_dir.fs, parent_dir.id, parent_index, id).await?;

		// Add ourselves to parent dir.
		// FIXME account for potential move while loading the directory.
		// The parent directory must hold a lock, but it currently is not.
		let r = parent_dir
			.fs
			.dir_data(parent_dir.id)
			.children
			.insert(parent_index, Child::Dir(id));
		debug_assert!(r.is_none(), "child present in parent");

		Ok(dir_ref)
	}

	/// Load the root directory.
	pub(crate) async fn load_root(fs: &'a Nrfs<D>) -> Result<DirRef<'a, D>, Error<D>> {
		trace!("load_root");
		// Check if the root directory is already present in the filesystem object.
		//
		// If so, just reference that and return.
		if let Some(dir) = fs.data.borrow_mut().directories.get_mut(&ObjectId::ZERO) {
			dir.header.reference_count += 1;
			return Ok(DirRef { fs, id: 0 });
		}

		// FIXME check if the directory is already being loaded
		// Also create some guard when we start fetching directory data.

		// Load directory data.
		let dir_ref = Self::load_inner(fs, u64::MAX, u32::MAX, 0).await?;

		// FIXME ditto

		Ok(dir_ref)
	}

	/// Load an existing directory.
	///
	/// This does not directly create a reference to a parent directory.
	///
	/// # Note
	///
	/// This function does not check if a corresponding [`DirData`] is already present!
	async fn load_inner(
		fs: &'a Nrfs<D>,
		parent_id: ObjectId,
		parent_index: Index,
		id: ObjectId,
	) -> Result<DirRef<'a, D>, Error<D>> {
		trace!("load_inner {:?} | {:?}:{:?}", id, parent_id, parent_index);
		let obj = fs.get(id).await?;

		// Get basic info
		let mut buf = [0; 16];
		crate::read_exact(&obj, 0, &mut buf).await?;
		let [header_len8, item_len8, _, _, _, _, _, _, rem @ ..] = buf;
		let [a, b, rem @ ..] = rem;
		let group_count = u16::from_le_bytes([a, b]);
		let group_list_offset = Offset::from_raw(&rem);

		// Get extensions
		let mut unix_offset = None;
		let mut mtime_offset = None;
		let mut offt = 16;
		// An extension consists of at least two bytes, ergo +1
		while offt + 1 < u16::from(header_len8) * 8 {
			let mut buf = [0; 2];
			crate::read_exact(&obj, offt.into(), &mut buf).await?;

			let [name_len, data_len] = buf;
			let total_len = u16::from(name_len) + u16::from(data_len);

			let buf = &mut *vec![0; total_len.into()];
			crate::read_exact(&obj, u64::from(offt) + 2, buf).await?;

			let (name, data) = buf.split_at(name_len.into());
			match name {
				b"unix" => {
					assert!(data_len >= 2, "data too short for unix");
					unix_offset = Some(u16::from_le_bytes([data[0], data[1]]))
				}
				b"mtime" => {
					assert!(data_len >= 2, "data too short for mtime");
					mtime_offset = Some(u16::from_le_bytes([data[0], data[1]]))
				}
				_ => {}
			}
			offt += 2 + total_len;
		}

		let data = DirData {
			header: DataHeader::new(parent_id, parent_index),
			children: Default::default(),
			header_len8,
			item_len8,
			hashmap_size,
			hasher: Hasher::from_raw(hash_algorithm, &hash_key).unwrap(), // TODO
			item_count,
			item_capacity,
			unix_offset,
			mtime_offset,
			heap_alloc_map: None,
			item_alloc_map: None,
			is_dangling: false,
		};

		// Insert directory data & return reference.
		fs.data.borrow_mut().directories.insert(id, data);
		Ok(Self { fs, id })
	}

	/// Get a list of enabled extensions.
	///
	/// This only includes extensions that are understood by this library.
	pub fn enabled_extensions(&self) -> EnableExtensions {
		let data = self.fs.dir_data(self.id);
		let mut ext = EnableExtensions::default();
		data.mtime_offset().map(|_| ext.add_mtime());
		data.unix_offset().map(|_| ext.add_unix());
		ext
	}

	/// Create a new file.
	///
	/// This fails if an entry with the given name already exists.
	pub async fn create_file(
		&self,
		name: &Name,
		ext: &Extensions,
	) -> Result<Result<FileRef<'a, D>, InsertError>, Error<D>> {
		trace!("create_file {:?}", name);
		let ty = Type::EmbedFile { length: 0 };
		let index = self.dir().insert(name, ty, ext).await?;
		Ok(index.map(|i| FileRef::from_embed(&self.dir(), 0, 0, i)))
	}

	/// Create a new directory.
	///
	/// This fails if an entry with the given name already exists.
	pub async fn create_dir(
		&self,
		name: &Name,
		options: &DirOptions,
		ext: &Extensions,
	) -> Result<Result<DirRef<'a, D>, InsertError>, Error<D>> {
		trace!("create_dir {:?}", name);
		// Try to insert stub entry
		let ty = Type::Dir { id: [u8::MAX; 7], item_count: [0; 3] };
		let index = match self.dir().insert(name, ty, ext).await? {
			Ok(i) => i,
			Err(e) => return Ok(Err(e)),
		};
		// Create new directory with stub index (u32::MAX).
		let d = DirRef::new(&self.dir(), index, options).await?;
		// Fixup ID in entry.
		self.dir()
			.set_ty(index, Type::Dir { id: d.id, item_count: [0; 3] })
			.await?;
		// Done!
		Ok(Ok(d))
	}

	/// Create a new symbolic link.
	///
	/// This fails if an entry with the given name already exists.
	pub async fn create_sym(
		&self,
		name: &Name,
		ext: &Extensions,
	) -> Result<Result<SymRef<'a, D>, InsertError>, Error<D>> {
		trace!("create_sym {:?}", name);
		let ty = Type::EmbedSym { offset: Offset::MIN, length: 0 };
		let index = self.dir().insert(name, ty, ext).await?;
		Ok(index.map(|i| SymRef::from_embed(&self.dir(), 0, 0, i)))
	}

	/// Retrieve the entry with an index equal or greater than `index`.
	///
	/// Used for iteration.
	pub async fn next_from(
		&self,
		mut index: u64,
	) -> Result<Option<(ItemRef<'a, D>, u64)>, Error<D>> {
		trace!("next_from {:?}", index);
		while index < u64::from(self.fs.dir_data(self.id).item_capacity) {
			// Get standard info
			let item = self.dir().get(index).await?;

			if matches!(item.ty, Type::None) {
				// Not in use, so skip.
				index += 1;
				continue;
			}

			let item = ItemRef::new(&self.dir(), &item).await?;
			return Ok(Some((item, index + 1)));
		}
		Ok(None)
	}

	/// Find an entry with the given name.
	pub async fn find(&self, name: &Name) -> Result<Option<ItemRef<'a, D>>, Error<D>> {
		trace!("find {:?}", name);
		let Some((index, _)) = self.dir().find(name).await?
			else { return Ok(None) };
		let item = self.dir().get(entry.item_index).await?;
		Ok(Some(ItemRef::new(&dir, &item).await?))
	}

	/// Rename an entry.
	///
	/// Returns `false` if the entry could not be found or another entry with the same index
	/// exists.
	pub async fn rename(
		&self,
		from: &Name,
		to: &Name,
	) -> Result<Result<(), RenameError>, Error<D>> {
		self.dir().rename(from, to).await
	}

	/// Move an entry to another directory.
	///
	/// Returns `false` if the entry could not be found or another entry with the same index
	/// exists.
	///
	/// # Panics
	///
	/// If `self` and `to_dir` are on different filesystems.
	pub async fn transfer(
		&self,
		name: &Name,
		to_dir: &DirRef<'a, D>,
		to_name: &Name,
	) -> Result<Result<(), TransferError>, Error<D>> {
		assert_eq!(
			self.fs as *const _, to_dir.fs as *const _,
			"self and to_dir are on different filesystems"
		);
		self.dir().transfer(name, to_dir.id, to_name).await
	}

	/// Remove the entry with the given name.
	///
	/// Returns `Ok(Ok(()))` if successful.
	/// It will fail if no entry with the given name could be found.
	/// It will also fail if the type is unknown to avoid space leaks.
	///
	/// If there is a live reference to the removed item,
	/// the item is kept intact until all references are dropped.
	pub async fn remove(&self, name: &Name) -> Result<Result<(), RemoveError>, Error<D>> {
		trace!("remove {:?}", name);
		let Some((index, _)) = self.dir().find(name).await?
			else { return Ok(Err(RemoveError::NotFound)) };
		self.dir().remove_at(index).await?;
		Ok(Ok(()))
	}

	/// Destroy the reference to this directory.
	///
	/// This will perform cleanup if the directory is dangling
	/// and this was the last reference.
	pub async fn drop(mut self) -> Result<(), Error<D>> {
		trace!("drop");
		// Loop so we can drop references to parent directories
		// without recursion and avoid a potential stack overflow.
		loop {
			// Don't run the Drop impl
			let DirRef { id, fs } = self;
			mem::forget(self);

			let mut fs_ref = fs.data.borrow_mut();
			let btree_map::Entry::Occupied(mut data) = fs_ref.directories.entry(id) else {
				unreachable!()
			};
			data.get_mut().header.reference_count -= 1;
			if data.get().header.reference_count == 0 {
				// Remove DirData.
				let header = data.get().header.clone();
				let data = data.remove();

				// If this is the root dir there is no parent dir,
				// so check first.
				if self.id != ObjectId::ROOT {
					// Remove itself from parent directory.
					let dir = fs_ref
						.directories
						.get_mut(&header.parent_id)
						.expect("parent dir is not loaded");
					let r = dir.children.remove(&header.parent_index);
					debug_assert!(
						matches!(r, Some(Child::Dir(i)) if i == id),
						"child not present in parent"
					);

					drop(fs_ref);

					// Reconstruct DirRef to adjust reference count of dir appropriately.
					self = DirRef { fs, id: header.parent_id };

					// If this directory is dangling, destroy it
					// and remove it from the parent directory.
					let r = async {
						if data.is_dangling && !fs.read_only {
							fs.get(id).await?.dealloc().await?;
							self.dir().clear_item(data.header.parent_index).await?;
						}
						Ok(())
					}
					.await;
					if let Err(e) = r {
						mem::forget(self);
						return Err(e);
					}

					// Continue with parent dir
					continue;
				} else {
					debug_assert!(!data.is_dangling, "root cannot dangle");
				}
			}
			break;
		}
		Ok(())
	}

	/// Get item data, i.e. data in the entry itself, excluding heap data.
	pub async fn data(&self) -> Result<ItemData, Error<D>> {
		TmpRef::<'_, ItemRef<'_, D>>::from(self).data().await
	}

	/// Get the amount of items in this directory.
	pub async fn len(&self) -> Result<u32, Error<D>> {
		Ok(self.fs.dir_data(self.id).item_count)
	}

	/// Create a [`Dir`] helper structure.
	pub(crate) fn dir(&self) -> Dir<'a, D> {
		Dir::new(self.fs, self.id)
	}
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(any(test, fuzzing), derive(arbitrary::Arbitrary))]
pub struct DirOptions {
	pub extensions: EnableExtensions,
	pub hasher: Hasher,
}

impl DirOptions {
	/// Initialize directory options with default settings and the supplied hash key.
	///
	/// It is an alternative to [`Default`] which forces a key to be provided.
	pub fn new(key: &[u8; 16]) -> Self {
		Self { extensions: Default::default(), hasher: Hasher::SipHasher13(*key) }
	}
}

macro_rules! n2e {
	(@INTERNAL $op:ident :: $fn:ident $int:ident $name:ident) => {
		impl core::ops::$op<$name> for $int {
			type Output = $int;

			fn $fn(self, rhs: $name) -> Self::Output {
				self.$fn(rhs.to_raw())
			}
		}
	};
	{
		$(#[doc = $doc:literal])*
		[$name:ident]
		$($v:literal $k:ident)*
	} => {
		$(#[doc = $doc])*
		#[derive(Clone, Copy, Default, Debug)]
		#[cfg_attr(any(test, fuzzing), derive(arbitrary::Arbitrary))]
		pub enum $name {
			#[default]
			$($k = $v,)*
		}

		impl $name {
			pub fn from_raw(n: u8) -> Option<Self> {
				Some(match n {
					$($v => Self::$k,)*
					_ => return None,
				})
			}

			pub fn to_raw(self) -> u8 {
				self as _
			}
		}

		n2e!(@INTERNAL Shl::shl u64 $name);
		n2e!(@INTERNAL Shr::shr u64 $name);
		n2e!(@INTERNAL Shl::shl usize $name);
		n2e!(@INTERNAL Shr::shr usize $name);
	};
}

n2e! {
	/// The capacity of the directory.
	[DirSize]
	0 B1
	1 B2
	2 B4
	3 B8
	4 B16
	5 B32
	6 B64
	7 B128
	8 B256
	9 B512
	10 K1
	11 K2
	12 K4
	13 K8
	14 K16
	15 K32
	16 K64
	17 K128
	18 K256
	19 K512
	20 M1
	21 M2
	22 M4
	23 M8
	24 M16
	25 M32
	26 M64
	27 M128
	28 M256
	29 M512
	30 G1
	31 G2
}

#[derive(Clone, Copy, Default, Debug)]
#[cfg_attr(any(test, fuzzing), derive(arbitrary::Arbitrary))]
pub struct EnableExtensions(u8);

macro_rules! ext {
	($a:ident $g:ident $b:literal) => {
		pub fn $a(&mut self) -> &mut Self {
			self.0 |= 1 << $b;
			self
		}

		pub fn $g(&self) -> bool {
			self.0 & 1 << $b != 0
		}
	};
}

impl EnableExtensions {
	ext!(add_unix unix 0);
	ext!(add_mtime mtime 1);
}

#[derive(Default, Debug)]
#[cfg_attr(any(test, fuzzing), derive(arbitrary::Arbitrary))]
pub struct Extensions {
	pub unix: Option<ext::unix::Entry>,
	pub mtime: Option<ext::mtime::Entry>,
}

impl Extensions {
	/// Clear or initialize any extension data depending on state in [`EnableExtensions`].
	pub fn mask(&mut self, enable: EnableExtensions) {
		self.unix = enable.unix().then(|| self.unix.unwrap_or_default());
		self.mtime = enable.mtime().then(|| self.mtime.unwrap_or_default());
	}
}

/// An error that occured while trying to remove an entry.
#[derive(Clone, Debug)]
pub enum RemoveError {
	/// The entry was not found.
	NotFound,
	/// The entry is a directory and was not empty.
	NotEmpty,
	/// The entry was not recognized.
	///
	/// Unrecognized entries aren't removed to avoid space leaks.
	UnknownType,
}

/// An error that occured while trying to insert an entry.
#[derive(Clone, Debug)]
pub enum InsertError {
	/// An entry with the same name already exists.
	Duplicate,
	/// The directory is full.
	Full,
	/// The directory was removed and does not accept new entries.
	Dangling,
}

/// An error that occured while trying to transfer an entry.
#[derive(Clone, Debug)]
pub enum TransferError {
	/// The entry was not found.
	NotFound,
	/// The entry is an ancestor of the directory it was about to be
	/// transferred to.
	IsAncestor,
	/// The entry was not recognized.
	///
	/// Unrecognized entries aren't removed to avoid space leaks.
	UnknownType,
	/// An entry with the same name already exists.
	Duplicate,
	/// The target directory is full.
	Full,
	/// The target directory was removed and does not accept new entries.
	Dangling,
}

/// An error that occured while trying to transfer an entry.
#[derive(Clone, Debug)]
pub enum RenameError {
	/// The entry was not found.
	NotFound,
	/// An entry with the same name already exists.
	Duplicate,
}
