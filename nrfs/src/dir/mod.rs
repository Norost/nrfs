pub mod ext;

mod hashmap;

use {
	crate::{Dev, Error, File, Name, Nrfs},
	core::{
		fmt,
		ops::{Deref, DerefMut},
	},
	hashmap::*,
	rangemap::RangeSet,
};

// TODO determine a good load factor.
const MAX_LOAD_FACTOR_MILLI: u64 = 875;
const MIN_LOAD_FACTOR_MILLI: u64 = 375;

const TY_FILE: u8 = 1;
const TY_DIR: u8 = 2;
const TY_SYM: u8 = 3;
const TY_EMBED_FILE: u8 = 4;
const TY_EMBED_SYM: u8 = 5;

pub struct Dir<'a, D: Dev> {
	pub(crate) fs: &'a mut Nrfs<D>,
	data: DirData,
}

/// Directory data only, which has no lifetimes.
pub struct DirData {
	id: u64,
	header_len8: u8,
	entry_len8: u8,
	hashmap_size_p2: u8,
	hash_key: [u8; 16],
	hash_algorithm: HashAlgorithm,
	entry_count: u32,
	unix_offset: Option<u16>,
	mtime_offset: Option<u16>,
	// Lazily load the allocation map to save time when only reading.
	alloc_map: Option<RangeSet<u64>>,
}

impl<D: Dev> Deref for Dir<'_, D> {
	type Target = DirData;

	fn deref(&self) -> &Self::Target {
		&self.data
	}
}

impl<D: Dev> DerefMut for Dir<'_, D> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.data
	}
}

impl<'a, D: Dev> Dir<'a, D> {
	/// Create a new directory.
	pub(crate) async fn new(
		fs: &'a mut Nrfs<D>,
		options: &DirOptions,
	) -> Result<Dir<'a, D>, Error<D>> {
		let mut header_len = 24;
		let mut entry_len = 24;
		let unix_offset = options.extensions.unix().then(|| {
			header_len += 8; // 4, 2, "unix", offset
			let o = entry_len;
			entry_len += 8;
			o
		});
		let mtime_offset = options.extensions.mtime().then(|| {
			header_len += 9; // 5, 2, "mtime", offset
			let o = entry_len;
			entry_len += 8;
			o
		});

		let mut slf = Self {
			fs,
			data: DirData {
				id: u64::MAX,
				header_len8: ((header_len + 7) / 8).try_into().unwrap(),
				entry_len8: ((entry_len + 7) / 8).try_into().unwrap(),
				hashmap_size_p2: options.capacity_p2,
				hash_key: options.hash_key,
				hash_algorithm: options.hash_algorithm,
				entry_count: 0,
				unix_offset,
				mtime_offset,
				alloc_map: Some(Default::default()),
			},
		};
		let (slf_obj, heap_obj) = slf.fs.storage.create_pair().await?;
		let slf_id = slf_obj.id();
		drop((slf_obj, heap_obj));
		slf.id = slf_id;
		slf.init_with_size(slf.id, options.capacity_p2).await?;
		Ok(slf)
	}

	/// Initialize a hashmap object with the given size.
	///
	/// This does not modify the current dir structure.
	async fn init_with_size(&mut self, id: u64, map_size_p2: u8) -> Result<(), Error<D>> {
		let mut buf = [0; 64];
		buf[0] = self.header_len8;
		buf[1] = self.entry_len8;
		buf[2] = self.hash_algorithm as _;
		buf[3] = map_size_p2;
		buf[4..8].copy_from_slice(&self.entry_count.to_le_bytes());
		buf[8..24].copy_from_slice(&self.hash_key);
		let mut header_offt = 24;

		let buf = &mut buf[..usize::from(self.header_len8) * 8];
		if let Some(offt) = self.unix_offset {
			buf[header_offt + 0] = 4; // name len
			buf[header_offt + 1] = 2; // data len
			buf[header_offt + 2..][..4].copy_from_slice(b"unix");
			buf[header_offt + 6..][..2].copy_from_slice(&offt.to_le_bytes());
			header_offt += 8;
		}
		if let Some(offt) = self.mtime_offset {
			buf[header_offt + 0] = 5; // name len
			buf[header_offt + 1] = 2; // data len
			buf[header_offt + 2..][..5].copy_from_slice(b"mtime");
			buf[header_offt + 7..][..2].copy_from_slice(&offt.to_le_bytes());
		}
		self.fs
			.storage
			.get(id)
			.await?
			.resize(self.hashmap_base() + self.entry_size() << map_size_p2)
			.await?;
		self.fs
			.write_all(id, 0, &buf[..usize::from(self.header_len8) * 8])
			.await?;

		Ok(())
	}

	pub(crate) async fn load(fs: &'a mut Nrfs<D>, id: u64) -> Result<Dir<'a, D>, Error<D>> {
		// Get basic info
		let mut buf = [0; 24];
		fs.read_exact(id, 0, &mut buf).await?;
		let [header_len8, entry_len8, hash_algorithm, hashmap_size_p2, a, b, c, d, hash_key @ ..] =
			buf;
		let entry_count = u32::from_le_bytes([a, b, c, d]);

		// Get extensions
		let mut unix_offset = None;
		let mut mtime_offset = None;
		let mut offt = 24;
		// An extension consists of at least two bytes, ergo +1
		while offt + 1 < u16::from(header_len8) * 8 {
			let mut buf = [0; 2];
			fs.read_exact(id, offt.into(), &mut buf).await?;
			let [name_len, data_len] = buf;
			let total_len = u16::from(name_len) + u16::from(data_len);
			let mut buf = [0; 255 * 2];
			fs.read_exact(id, u64::from(offt) + 2, &mut buf[..total_len.into()])
				.await?;
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

		Ok(Self {
			fs,
			data: DirData {
				id,
				header_len8,
				entry_len8,
				hashmap_size_p2,
				hash_algorithm: match hash_algorithm {
					1 => HashAlgorithm::SipHasher13,
					n => return Err(Error::UnknownHashAlgorithm(n)),
				},
				hash_key,
				entry_count,
				unix_offset,
				mtime_offset,
				alloc_map: None,
			},
		})
	}

	/// Create a new file.
	///
	/// This fails if an entry with the given name already exists.
	pub async fn create_file<'b>(
		&'b mut self,
		name: &Name,
		ext: &Extensions,
	) -> Result<Option<File<'a, 'b, D>>, Error<D>> {
		let e = NewEntry { name, ty: Type::EmbedFile { offset: 0, length: 0 } };
		self.insert(e, ext)
			.await
			.map(|r| r.map(|i| File::from_embed(self, false, i, 0, 0)))
	}

	/// Create a new directory.
	///
	/// This fails if an entry with the given name already exists.
	pub async fn create_dir<'s>(
		&'s mut self,
		name: &Name,
		options: &DirOptions,
		ext: &Extensions,
	) -> Result<Option<Dir<'s, D>>, Error<D>> {
		let d = Dir::new(self.fs, options).await?.data;
		let e = NewEntry { name, ty: Type::Dir { id: d.id } };
		self.insert(e, ext)
			.await
			.map(|r| r.map(|_| Dir { fs: self.fs, data: d }))
	}

	/// Create a new symbolic link.
	///
	/// This fails if an entry with the given name already exists.
	pub async fn create_sym<'b>(
		&'b mut self,
		name: &Name,
		ext: &Extensions,
	) -> Result<Option<File<'a, 'b, D>>, Error<D>> {
		let e = NewEntry { name, ty: Type::EmbedSym { offset: 0, length: 0 } };
		self.insert(e, ext)
			.await
			.map(|r| r.map(|i| File::from_embed(self, true, i, 0, 0)))
	}

	pub async fn next_from<'b>(
		&'b mut self,
		mut index: u32,
	) -> Result<Option<(Entry<'a, 'b, D>, Option<u32>)>, Error<D>> {
		while u64::from(index) < self.capacity() {
			// Get standard info
			let e = self.hashmap().get(index).await?;

			if e.ty == 0 {
				// Is empty, so skip
				index += 1;
				continue;
			}

			// Get key
			let mut key = [0; 255];
			let key = &mut key[..e.key_len.into()];
			self.read_heap(e.key_offset, key).await?;

			// Get extension info
			let e = self.hashmap().get_ext(index, e).await?;
			let e = Entry::new(self, e, key);
			return Ok(Some((e, index.checked_add(1))));
		}
		Ok(None)
	}

	pub async fn find<'b>(&'b mut self, name: &Name) -> Result<Option<Entry<'a, 'b, D>>, Error<D>> {
		if let Some((i, e)) = self.hashmap().find_index(name).await? {
			self.hashmap()
				.get_ext(i, e)
				.await
				.map(|e| Entry::new(self, e, name))
				.map(Some)
		} else {
			Ok(None)
		}
	}

	/// Try to insert a new entry.
	///
	/// Returns `None` if an entry with the same name already exists.
	async fn insert(
		&mut self,
		entry: NewEntry<'_>,
		ext: &Extensions,
	) -> Result<Option<u32>, Error<D>> {
		// Check if we should grow the hashmap
		if self.should_grow() {
			self.grow().await?;
		}

		let name = Some(entry.name);
		let entry = RawEntryExt {
			entry: RawEntry {
				key_len: u8::MAX,
				key_offset: u64::MAX,
				ty: entry.ty.to_ty(),
				id_or_offset: entry.ty.to_data(),
				index: u32::MAX,
				hash: 0,
			},
			unix: ext.unix,
			mtime: ext.mtime,
		};

		let r = self.hashmap().insert(entry, name).await?;
		if r.is_some() {
			self.set_entry_count(self.entry_count + 1).await?;
		}
		Ok(r)
	}

	/// Remove the entry with the given name.
	///
	/// # Note
	///
	/// While it does check if the entry is a directory, it does not check whether it's empty.
	/// It is up to the user to ensure the directory is empty.
	pub async fn remove(&mut self, name: &Name) -> Result<bool, Error<D>> {
		if let Some((i, e)) = self.hashmap().find_index(name).await? {
			self.remove_at(i, (e.key_offset, e.key_len), e.ty().unwrap())
				.await
				.map(|()| true)
		} else {
			Ok(false)
		}
	}

	async fn remove_at(&mut self, index: u32, key: (u64, u8), ty: Type) -> Result<(), Error<D>> {
		self.set_entry_count(self.entry_count - 1).await?;
		self.hashmap().remove_at(index).await?;

		// Deallocate string
		self.dealloc(key.0, key.1.into()).await?;

		match ty {
			Type::File { id } | Type::Sym { id } => {
				// Dereference object.
				self.fs.storage.decrease_reference_count(id).await?;
			}
			Type::Dir { id } => {
				// Dereference map and heap.
				self.fs.storage.decrease_reference_count(id).await?;
				self.fs.storage.decrease_reference_count(id + 1).await?;
			}
			Type::EmbedFile { offset, length } | Type::EmbedSym { offset, length } => {
				self.dealloc(offset, length.into()).await?;
			}
		}

		// Check if we should shrink the hashmap
		if self.should_shrink() {
			self.shrink().await?;
		}
		Ok(())
	}

	/// Set the type and offset of an entry.
	///
	/// The entry must not be empty, i.e. type is not 0.
	pub(crate) async fn set_ty(&mut self, index: u32, ty: Type) -> Result<(), Error<D>> {
		let mut e = self.hashmap().get(index).await?;
		debug_assert!(e.ty != 0);
		e.ty = ty.to_ty();
		e.id_or_offset = ty.to_data();
		self.hashmap().set(index, &e).await.map(|_: u64| ())
	}

	/// Check if the hashmap should grow.
	fn should_grow(&self) -> bool {
		self.index_mask() == self.entry_count
			|| u64::from(self.entry_count) * 1000
				> u64::from(self.capacity()) * MAX_LOAD_FACTOR_MILLI
	}

	/// Check if the hashmap should shrink.
	fn should_shrink(&self) -> bool {
		u64::from(self.entry_count) * 1000 < u64::from(self.capacity()) * MIN_LOAD_FACTOR_MILLI
	}

	/// The current size of the hashmap
	fn capacity(&self) -> u64 {
		1 << self.hashmap_size_p2
	}

	/// The size of the hashmap minus one
	fn index_mask(&self) -> u32 {
		(self.capacity() as u32).wrapping_sub(1)
	}

	/// Compare a stored name with the given name.
	async fn compare_names(&mut self, x: (u8, u64), y: &[u8]) -> Result<bool, Error<D>> {
		if usize::from(x.0) != y.len() {
			return Ok(false);
		}
		let mut buf = [0; 255];
		self.fs
			.read_exact(self.id + 1, x.1, &mut buf[..y.len()])
			.await?;
		Ok(&buf[..y.len()] == y)
	}

	/// Allocate heap space for arbitrary data.
	///
	/// The returned region is not readable until it is written to.
	pub(crate) async fn alloc(&mut self, len: u64) -> Result<u64, Error<D>> {
		if len == 0 {
			return Ok(0);
		}
		let log = self.alloc_log().await?;
		for r in log.gaps(&(0..u64::MAX)) {
			if r.end - r.start >= len {
				log.insert(r.start..r.start + len);
				self.save_alloc_log().await?;
				return Ok(r.start);
			}
		}
		// This is unreachable in practice.
		unreachable!("all 2^64 bytes are allocated");
	}

	/// Deallocate heap space.
	pub(crate) async fn dealloc(&mut self, offset: u64, len: u64) -> Result<(), Error<D>> {
		if len > 0 {
			let r = offset..offset + len;
			let log = self.alloc_log().await?;
			debug_assert!(
				log.iter().any(|d| r.clone().all(|e| d.contains(&e))),
				"double free"
			);
			log.remove(r);
			self.save_alloc_log().await?;
		}
		Ok(())
	}

	/// Write a full, minimized allocation log.
	async fn save_alloc_log(&mut self) -> Result<(), Error<D>> {
		let id = self.id;
		let mut log_offt = self.alloc_log_base();
		// Avoid mutable borrow issues
		self.alloc_log().await?;
		let log = self.data.alloc_map.as_mut().unwrap();
		self.fs
			.resize(id, log_offt + 16 * log.iter().size_hint().0 as u64)
			.await?;
		for r in log.iter() {
			let mut buf = [0; 16];
			buf[..8].copy_from_slice(&r.start.to_le_bytes());
			buf[8..].copy_from_slice(&(r.end - r.start).to_le_bytes());
			self.fs.write_all(id, log_offt, &buf).await?;
			log_offt += 16;
		}
		self.fs.resize(self.id, log_offt).await
	}

	/// Get or load the allocation map.
	async fn alloc_log(&mut self) -> Result<&mut RangeSet<u64>, Error<D>> {
		// I'd use as_mut() but the borrow checker has a bug :(
		if self.alloc_map.is_some() {
			return Ok(self.alloc_map.as_mut().unwrap());
		}
		let mut m = RangeSet::new();
		let l = self.fs.length(self.id).await?;
		for offt in (self.alloc_log_base()..l).step_by(16) {
			let mut buf = [0; 16];
			self.fs.read_exact(self.id, offt, &mut buf).await?;
			let [a, b, c, d, e, f, g, h, buf @ ..] = buf;
			let offset = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
			let len = u64::from_le_bytes(buf);
			if len & 1 << 63 != 0 {
				// Dealloc
				m.remove(offset..offset + (len ^ 1 << 63));
			} else {
				// Alloc
				m.insert(offset..offset + len);
			}
		}
		Ok(self.alloc_map.insert(m))
	}

	/// The base address of the hashmap.
	fn hashmap_base(&self) -> u64 {
		u64::from(self.header_len8) * 8
	}

	/// The base address of the allocation log.
	fn alloc_log_base(&self) -> u64 {
		self.hashmap_base() + self.entry_size() * self.capacity()
	}

	/// The size of a single entry.
	fn entry_size(&self) -> u64 {
		u64::from(self.entry_len8) * 8
	}

	/// Read a heap value.
	pub(crate) async fn read_heap(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), Error<D>> {
		self.fs.read_exact(self.id + 1, offset, buf).await
	}

	/// Write a heap value.
	pub(crate) async fn write_heap(&mut self, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		self.fs.write_grow(self.id + 1, offset, data).await
	}

	/// Grow the hashmap
	async fn grow(&mut self) -> Result<(), Error<D>> {
		debug_assert!(
			self.hashmap_size_p2 < 32,
			"hashmap is already at maximum size"
		);
		self.resize(self.hashmap_size_p2 + 1).await
	}

	/// Shrink the hashmap.
	///
	/// There must be *at least* `capacity / 2 + 1` slots free,
	/// i.e. `entry_count < capacity / 2`.
	async fn shrink(&mut self) -> Result<(), Error<D>> {
		debug_assert!(
			self.hashmap_size_p2 != 0,
			"hashmap is already at minimum size"
		);
		debug_assert!(
			u64::from(self.entry_count) < self.capacity() / 2,
			"not enough free slots"
		);
		self.resize(self.hashmap_size_p2 - 1).await
	}

	/// Resize the hashmap
	async fn resize(&mut self, new_size_p2: u8) -> Result<(), Error<D>> {
		// Since we're going to load the entire log we can as well minimize it.
		self.alloc_log().await?;

		let new_map_id = self.fs.storage.create().await?.id();
		self.init_with_size(new_map_id, new_size_p2).await?;

		// Copy entries
		for index in (0..self.capacity()).map(|i| i as _) {
			let e = self.hashmap().get(index).await?;
			if e.ty == 0 {
				continue;
			}
			let e = self.hashmap().get_ext(index, e).await?;
			HashMap::new(self, new_map_id, new_size_p2)
				.insert(e, None)
				.await?;
		}

		// Replace old map
		self.fs
			.storage
			.get(self.id)
			.await?
			.replace_with(self.fs.storage.get(new_map_id).await?)
			.await?;
		self.hashmap_size_p2 = new_size_p2;
		self.save_alloc_log().await
	}

	/// Update the entry count.
	async fn set_entry_count(&mut self, count: u32) -> Result<(), Error<D>> {
		self.fs.write_all(self.id, 4, &count.to_le_bytes()).await?;
		self.entry_count = count;
		Ok(())
	}

	fn hashmap(&mut self) -> HashMap<'a, '_, D> {
		HashMap::new(self, self.id, self.hashmap_size_p2)
	}

	pub fn into_data(self) -> DirData {
		self.data
	}

	pub fn from_data(fs: &'a mut Nrfs<D>, data: DirData) -> Self {
		Self { fs, data }
	}

	pub async fn transfer(
		&mut self,
		name: &Name,
		to_dir: &mut DirData,
		to_name: &Name,
	) -> Result<bool, Error<D>> {
		if let Some((i, e)) = self.hashmap().find_index(name).await? {
			let e = self.hashmap().get_ext(i, e).await?;
			core::mem::swap(&mut self.data, to_dir);
			if self.should_grow() {
				if let Err(e) = self.grow().await {
					core::mem::swap(&mut self.data, to_dir);
					return Err(e);
				}
			}
			let r = match self.hashmap().insert(e, Some(to_name)).await {
				Ok(r) => r,
				Err(e) => {
					core::mem::swap(&mut self.data, to_dir);
					return Err(e);
				}
			};
			if r.is_some() {
				let r = self.set_entry_count(self.entry_count + 1).await;
				core::mem::swap(&mut self.data, to_dir);
				r?;
				self.hashmap().remove_at(i).await?;
				self.set_entry_count(self.entry_count - 1).await?;
				return Ok(true);
			}
		}
		Ok(false)
	}

	pub async fn rename(&mut self, from: &Name, to: &Name) -> Result<bool, Error<D>> {
		if let Some((i, e)) = self.hashmap().find_index(from).await? {
			let e = self.hashmap().get_ext(i, e).await?;
			// Resizing is not necessary as there is guaranteed to be a free spot
			// and we'll free another spot if the insert succeeds.
			let r = self.hashmap().insert(e, Some(to)).await;
			if r?.is_some() {
				self.hashmap().remove_at(i).await?;
				return Ok(true);
			}
		}
		Ok(false)
	}
}

impl DirData {
	pub fn len(&self) -> u32 {
		self.entry_count
	}

	pub fn id(&self) -> u64 {
		self.id
	}
}

impl<D: Dev> fmt::Debug for Dir<'_, D>
where
	Nrfs<D>: fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Dir))
			.field("id", &self.id)
			.field("header_len", &(u16::from(self.header_len8) * 8))
			.field("entry_len", &(u16::from(self.entry_len8) * 8))
			.field(
				"hashmap_size_p2",
				&format_args!("2**{}", self.hashmap_size_p2),
			)
			.field("hash_algorithm", &self.hash_algorithm)
			.field("entry_count", &self.entry_count)
			.field("unix_offset", &self.unix_offset)
			.field("mtime_offset", &self.mtime_offset)
			.field("alloc_map", &self.alloc_map)
			.field("fs", &self.fs)
			.finish_non_exhaustive()
	}
}

pub struct Entry<'a, 'b, D: Dev> {
	dir: &'b mut Dir<'a, D>,
	index: u32,
	hash: u32,
	ty: Result<Type, u8>,
	key_len: u8,
	key_offset: u64,
	key: [u8; 255],
	unix: Option<ext::unix::Entry>,
	mtime: Option<ext::mtime::Entry>,
}

impl<'a, 'b, D: Dev> Entry<'a, 'b, D> {
	fn new(dir: &'b mut Dir<'a, D>, e: RawEntryExt, name: &[u8]) -> Self {
		debug_assert_eq!(usize::from(e.entry.key_len), name.len());
		let mut key = [0; 255];
		key[..name.len()].copy_from_slice(name);
		Self {
			dir,
			index: e.entry.index,
			hash: e.entry.hash,
			ty: e.entry.ty(),
			key_len: e.entry.key_len,
			key_offset: e.entry.key_offset,
			key,
			unix: e.unix,
			mtime: e.mtime,
		}
	}

	pub fn name(&self) -> &Name {
		self.key[..self.key_len.into()].try_into().unwrap()
	}

	pub fn is_file(&self) -> bool {
		matches!(&self.ty, Ok(Type::File { .. }) | Ok(Type::EmbedFile { .. }))
	}

	pub fn is_dir(&self) -> bool {
		matches!(&self.ty, Ok(Type::Dir { .. }))
	}

	pub fn is_sym(&self) -> bool {
		matches!(&self.ty, Ok(Type::Sym { .. }) | Ok(Type::EmbedSym { .. }))
	}

	pub fn as_file(&mut self) -> Option<File<'a, '_, D>> {
		Some(match self.ty {
			Ok(Type::File { id }) => File::from_obj(self.dir, false, id, self.index),
			Ok(Type::EmbedFile { offset, length }) => {
				File::from_embed(self.dir, false, self.index, offset, length)
			}
			_ => return None,
		})
	}

	pub async fn as_dir(&mut self) -> Option<Result<Dir<'_, D>, Error<D>>> {
		Some(match self.ty {
			Ok(Type::Dir { id }) => Dir::load(self.dir.fs, id).await,
			_ => return None,
		})
	}

	pub fn as_sym(&mut self) -> Option<File<'a, '_, D>> {
		Some(match self.ty {
			Ok(Type::Sym { id }) => File::from_obj(self.dir, true, id, self.index),
			Ok(Type::EmbedSym { offset, length }) => {
				File::from_embed(self.dir, true, self.index, offset, length)
			}
			_ => return None,
		})
	}

	pub fn dir_id(&self) -> Option<u64> {
		match self.ty {
			Ok(Type::Dir { id }) => Some(id),
			_ => None,
		}
	}

	pub async fn remove(self) -> Result<(), Error<D>> {
		self.dir
			.remove_at(
				self.index,
				(self.key_offset, self.key_len),
				self.ty.unwrap(), // TODO handle unknown entry types gracefully
			)
			.await
	}

	pub fn ext_unix(&self) -> Option<&ext::unix::Entry> {
		self.unix.as_ref()
	}

	pub fn ext_mtime(&self) -> Option<&ext::mtime::Entry> {
		self.mtime.as_ref()
	}

	pub async fn ext_set_unix(&mut self, unix: ext::unix::Entry) -> Result<bool, Error<D>> {
		let r = self.dir.ext_set_unix(self.index, unix).await?;
		self.unix = r.then(|| unix);
		Ok(r)
	}

	pub async fn ext_set_mtime(&mut self, mtime: ext::mtime::Entry) -> Result<bool, Error<D>> {
		let r = self.dir.ext_set_mtime(self.index, mtime).await?;
		self.mtime = r.then(|| mtime);
		Ok(r)
	}

	pub fn is_embedded(&self) -> bool {
		match &self.ty {
			Ok(Type::EmbedSym { .. }) | Ok(Type::EmbedFile { .. }) => true,
			_ => false,
		}
	}
}

impl<D: Dev + fmt::Debug> fmt::Debug for Entry<'_, '_, D> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Entry))
			.field("index", &self.index)
			.field("hash", &format_args!("{:#10x}", self.hash))
			.field("ty", &self.ty)
			// TODO use Utf8Lossy when it is stable.
			.field(
				"key",
				&String::from_utf8_lossy(&self.key[..self.key_len.into()]),
			)
			.field("key_offset", &self.key_offset)
			.field("key_len", &self.key_len)
			.field("unix", &self.unix)
			.finish_non_exhaustive()
	}
}

#[derive(Debug)]
pub(crate) enum Type {
	File { id: u64 },
	Dir { id: u64 },
	Sym { id: u64 },
	EmbedFile { offset: u64, length: u16 },
	EmbedSym { offset: u64, length: u16 },
}

impl Type {
	fn to_ty(&self) -> u8 {
		match self {
			Self::File { .. } => TY_FILE,
			Self::Dir { .. } => TY_DIR,
			Self::Sym { .. } => TY_SYM,
			Self::EmbedFile { .. } => TY_EMBED_FILE,
			Self::EmbedSym { .. } => TY_EMBED_SYM,
		}
	}

	fn to_data(&self) -> u64 {
		match self {
			Self::File { id } | Self::Dir { id } | Self::Sym { id } => *id,
			Self::EmbedFile { offset, length } | Self::EmbedSym { offset, length } => {
				*offset | u64::from(*length) << 48
			}
		}
	}
}

struct NewEntry<'a> {
	name: &'a Name,
	ty: Type,
}

#[derive(Default)]
pub struct DirOptions {
	pub capacity_p2: u8,
	pub extensions: EnableExtensions,
	pub hash_key: [u8; 16],
	pub hash_algorithm: HashAlgorithm,
}

#[derive(Clone, Copy, Default)]
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
pub struct Extensions {
	pub unix: Option<ext::unix::Entry>,
	pub mtime: Option<ext::mtime::Entry>,
}
