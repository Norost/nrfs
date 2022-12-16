pub mod ext;

mod hashmap;

use {
	crate::{
		read_exact, write_all, Dev, DirRef, Error, FileRef, Idx, Name, Nrfs, SymRef,
		UnknownRef,
	},
	core::{cell::RefMut, hash::Hasher},
	hashmap::*,
	rangemap::RangeSet,
	rustc_hash::FxHashMap,
	siphasher::sip::SipHasher13,
};

// TODO determine a good load factor.
const MAX_LOAD_FACTOR_MILLI: u64 = 875;
const MIN_LOAD_FACTOR_MILLI: u64 = 375;

const TY_NONE: u8 = 0;
const TY_FILE: u8 = 1;
const TY_DIR: u8 = 2;
const TY_SYM: u8 = 3;
const TY_EMBED_FILE: u8 = 4;
const TY_EMBED_SYM: u8 = 5;

/// Directory data only, which has no lifetimes.
///
/// The map is located at ID.
/// The heap is located at ID + 1.
#[derive(Debug)]
pub struct DirData {
	/// The amount of live [`DirRef`]s to this directory.
	pub(crate) reference_count: usize,
	/// Live [`FileRef`] and [`DirRef`]s that point to files which are a child of this directory.
	///
	/// Indexed by the index of the file on the on-disk hashmap.
	pub(crate) children: FxHashMap<u32, Child>,
	/// ID of the parent directory.
	///
	/// Not applicable if the ID of this directory is 0,
	/// i.e. it is the root directory.
	pub(crate) parent_id: u64,
	/// Index in the parent directory.
	///
	/// Not applicable if the ID of this directory is 0,
	/// i.e. it is the root directory.
	pub(crate) parent_index: u32,
	/// The length of the header, in multiples of 8 bytes.
	header_len8: u8,
	/// The length of a single entry, in multiples of 8 bytes.
	entry_len8: u8,
	/// The size of the hashmap, as a power of 2.
	///
	/// This is always between `0` and `32`.
	hashmap_size_p2: u8,
	/// The key to use with the hashing algorithm.
	///
	/// Necessary for some cryptographic hashes such as [`SipHasher13`].
	hash_key: [u8; 16],
	/// The hash algorithm used to index the hashmap.
	hash_algorithm: HashAlgorithm,
	/// The amount of entries in the hashmap.
	entry_count: u32,
	/// The offset of `unix` extension data, if in use.
	unix_offset: Option<u16>,
	/// The offset of `mtime` extension data, if in use.
	mtime_offset: Option<u16>,
	/// Allocation map of the heap.
	///
	/// This map is lazily loaded to save time when only reading the directory.
	alloc_map: Option<RangeSet<u64>>,
}

impl DirData {
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

	/// The size of the hashmap minus one.
	///
	/// Used to truncate indices.
	fn index_mask(&self) -> u32 {
		(self.capacity() as u32).wrapping_sub(1)
	}

	/// Hash an arbitrary-sized key.
	fn hash(&self, key: &[u8]) -> u32 {
		match self.hash_algorithm {
			HashAlgorithm::SipHasher13 => {
				let mut h = SipHasher13::new_with_key(&self.hash_key);
				h.write(key);
				h.finish() as _
			}
		}
	}
}

/// Helper structure for working with directories.
#[derive(Debug)]
pub(crate) struct Dir<'a, D: Dev> {
	/// The filesystem containing the directory's data.
	pub(crate) fs: &'a Nrfs<D>,
	/// The ID of this directory's map.
	///
	/// The heap is located at ID + 1.
	pub(crate) id: u64,
}

impl<'a, D: Dev> Dir<'a, D> {
	/// Create a [`Dir`] helper structure.
	pub(crate) fn new(fs: &'a Nrfs<D>, id: u64) -> Self {
		Self { fs, id }
	}

	/// Initialize a hashmap object with the given size.
	///
	/// This does not modify the current dir structure.
	async fn init_with_size(
		&self,
		object: &nros::Tree<'a, D>,
		map_size_p2: u8,
	) -> Result<(), Error<D>> {
		// Create header
		let mut data = self.fs.dir_data(self.id);
		let mut buf = [0; 64];
		buf[0] = data.header_len8;
		buf[1] = data.entry_len8;
		buf[2] = data.hash_algorithm as _;
		buf[3] = map_size_p2;
		buf[4..8].copy_from_slice(&data.entry_count.to_le_bytes());
		buf[8..24].copy_from_slice(&data.hash_key);
		let mut header_offt = 24;

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
		let (hashmap_base, entry_size) = (data.hashmap_base(), data.entry_size());
		let header_len = usize::from(data.header_len8) * 8;
		drop(data);
		object
			.resize(hashmap_base + (entry_size << map_size_p2))
			.await?;
		object.write(0, &buf[..header_len]).await?;

		Ok(())
	}

	/// Create a helper structure to operate on the hashmap of this directory.
	async fn hashmap(&self) -> Result<HashMap<'a, D>, Error<D>> {
		let obj = self.fs.storage.get(self.id).await?;
		Ok(HashMap::new(
			self,
			obj,
			self.fs.dir_data(self.id).hashmap_size_p2,
		))
	}

	/// Set the type and offset of an entry.
	///
	/// The entry must not be empty, i.e. type is not 0.
	pub(crate) async fn set_ty(&self, index: u32, ty: Type) -> Result<(), Error<D>> {
		let map = self.hashmap().await?;
		let mut e = map.get(index).await?;
		debug_assert!(e.ty != 0);
		e.ty = ty.to_ty();
		e.id_or_offset = ty.to_data();
		map.set(index, &e).await.map(|_: u64| ())
	}

	/// Read a heap value.
	pub(crate) async fn read_heap(&self, offset: u64, buf: &mut [u8]) -> Result<(), Error<D>> {
		self.fs.read_exact(self.id + 1, offset, buf).await
	}

	/// Write a heap value.
	pub(crate) async fn write_heap(&self, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		self.fs.write_grow(self.id + 1, offset, data).await
	}

	/// Allocate heap space for arbitrary data.
	///
	/// The returned region is not readable until it is written to.
	pub(crate) async fn alloc(&self, len: u64) -> Result<u64, Error<D>> {
		if len == 0 {
			return Ok(0);
		}
		let mut log = self.alloc_log().await?;
		for r in log.gaps(&(0..u64::MAX)) {
			if r.end - r.start >= len {
				log.insert(r.start..r.start + len);
				drop(log);
				self.save_alloc_log().await?;
				return Ok(r.start);
			}
		}
		// This is unreachable in practice.
		unreachable!("all 2^64 bytes are allocated");
	}

	/// Deallocate heap space.
	pub(crate) async fn dealloc(&self, offset: u64, len: u64) -> Result<(), Error<D>> {
		if len > 0 {
			let r = offset..offset + len;
			let mut log = self.alloc_log().await?;
			debug_assert!(
				log.iter().any(|d| r.clone().all(|e| d.contains(&e))),
				"double free"
			);
			log.remove(r);
			drop(log);
			self.save_alloc_log().await?;
		}
		Ok(())
	}

	/// Compare a stored name with the given name.
	async fn compare_names(&self, x: (u8, u64), y: &[u8]) -> Result<bool, Error<D>> {
		if usize::from(x.0) != y.len() {
			return Ok(false);
		}
		let mut buf = [0; 255];
		self.fs
			.read_exact(self.id + 1, x.1, &mut buf[..y.len()])
			.await?;
		Ok(&buf[..y.len()] == y)
	}

	/// Remove the entry with the given name.
	///
	/// # Note
	///
	/// While it does check if the entry is a directory, it does not check whether it's empty.
	/// It is up to the user to ensure the directory is empty.
	pub async fn remove(&self, name: &Name) -> Result<bool, Error<D>> {
		if let Some((i, e)) = self.hashmap().await?.find_index(name).await? {
			self.remove_at(i, (e.key_offset, e.key_len), e.ty().unwrap())
				.await
				.map(|()| true)
		} else {
			Ok(false)
		}
	}

	/// Remove an entry at a specific index.
	async fn remove_at(&self, index: u32, key: (u64, u8), ty: Type) -> Result<(), Error<D>> {
		self.update_entry_count(|x| x - 1).await?;
		self.hashmap().await?.remove_at(index).await?;

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
				// Free heap space.
				self.dealloc(offset, length.into()).await?;
			}
		}

		// Check if we should shrink the hashmap
		if self.fs.dir_data(self.id).should_shrink() {
			self.shrink().await?;
		}
		Ok(())
	}

	/// Rename an entry.
	async fn rename(&self, from: &Name, to: &Name) -> Result<bool, Error<D>> {
		let map = self.hashmap().await?;
		if let Some((i, e)) = map.find_index(from).await? {
			let e = map.get_ext(i, e).await?;
			// Resizing is not necessary as there is guaranteed to be a free spot
			// and we'll free another spot if the insert succeeds.
			let r = map.insert(e, Some(to)).await;
			if r?.is_some() {
				map.remove_at(i).await?;
				return Ok(true);
			}
		}
		Ok(false)
	}

	/// Update the entry count.
	async fn update_entry_count(&self, f: impl FnOnce(u32) -> u32) -> Result<(), Error<D>> {
		let mut data = self.fs.dir_data(self.id);
		let count = f(data.entry_count);
		data.entry_count = count;
		drop(data);
		self.fs.write_all(self.id, 4, &count.to_le_bytes()).await
	}

	/// Move an entry to another directory.
	async fn transfer(&self, name: &Name, to_dir: u64, to_name: &Name) -> Result<bool, Error<D>> {
		let to_dir = Dir::new(self.fs, to_dir);

		let from_map = self.hashmap().await?;

		// Find the entry to transfer.
		let Some((i, e)) = from_map.find_index(name).await? else { return Ok(false) };
		let e = from_map.get_ext(i, e).await?;

		// Check if the destination directory has enough capacity.
		// If not, grow it first.
		if self.fs.dir_data(to_dir.id).should_grow() {
			to_dir.grow().await?;
		}
		let to_map = to_dir.hashmap().await?;

		// Insert the entry in the destination directory.
		let Some(_) = to_map.insert(e, Some(to_name)).await? else { return Ok(false) };
		to_dir.update_entry_count(|x| x + 1).await?;

		// Remove the entry from the current directory.
		from_map.remove_at(i).await?;
		self.update_entry_count(|x| x - 1).await?;

		Ok(true)
	}

	/// Resize the hashmap
	///
	/// `grow` indicates whether the size of the map should increase or decrease.
	async fn resize(&self, grow: bool) -> Result<(), Error<D>> {
		// Since we're going to load the entire log we can as well minimize it.
		self.alloc_log().await?;

		let data = self.fs.dir_data(self.id);

		let hashmap_size_p2 = data.hashmap_size_p2;
		let capacity = data.capacity();
		let entry_count = data.entry_count;

		let new_size_p2 = if grow {
			debug_assert!(hashmap_size_p2 < 32, "hashmap is already at maximum size");
			hashmap_size_p2 + 1
		} else {
			debug_assert!(hashmap_size_p2 > 0, "hashmap is already at minimum size");
			debug_assert!(
				u64::from(entry_count) < capacity / 2,
				"not enough free slots"
			);
			hashmap_size_p2 - 1
		};
		drop(data);

		let new_map = self.fs.storage.create().await?;
		self.init_with_size(&new_map, new_size_p2).await?;

		// Copy entries
		let cur_map = self.hashmap().await?;
		let new_map = HashMap::new(self, new_map, new_size_p2);
		for index in (0..capacity).map(|i| i as _) {
			let e = cur_map.get(index).await?;
			if e.ty == 0 {
				continue;
			}
			let e = cur_map.get_ext(index, e).await?;
			new_map.insert(e, None).await?;
		}

		// Replace old map
		self.fs
			.storage
			.get(self.id)
			.await?
			.replace_with(new_map.map)
			.await?;
		self.fs.dir_data(self.id).hashmap_size_p2 = new_size_p2;
		self.save_alloc_log().await
	}

	/// Grow the hashmap
	async fn grow(&self) -> Result<(), Error<D>> {
		self.resize(true).await
	}

	/// Shrink the hashmap.
	///
	/// There must be *at least* `capacity / 2 + 1` slots free,
	/// i.e. `entry_count < capacity / 2`.
	async fn shrink(&self) -> Result<(), Error<D>> {
		self.resize(false).await
	}

	/// Try to insert a new entry.
	///
	/// Returns `None` if an entry with the same name already exists.
	async fn insert<'x>(
		&'x self,
		entry: NewEntry<'x>,
		ext: &'x Extensions,
	) -> Result<Option<u32>, Error<D>> {
		// Check if we should grow the hashmap
		if self.fs.dir_data(self.id).should_grow() {
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

		let r = self.hashmap().await?.insert(entry, name).await?;
		if r.is_some() {
			self.update_entry_count(|x| x + 1).await?;
		}
		Ok(r)
	}

	/// Write a full, minimized allocation log.
	async fn save_alloc_log(&self) -> Result<(), Error<D>> {
		// Get log.
		let log = self.alloc_log().await?.clone();
		let mut log_offt = self.fs.dir_data(self.id).alloc_log_base();
		let log_len = log.iter().count();

		// Ensure there is enough capacity.
		let map = self.fs.storage.get(self.id).await?;
		map.resize(log_offt + 16 * u64::try_from(log_len).unwrap())
			.await?;

		// Write log.
		for r in log.iter() {
			let mut buf = [0; 16];
			buf[..8].copy_from_slice(&r.start.to_le_bytes());
			buf[8..].copy_from_slice(&(r.end - r.start).to_le_bytes());
			write_all(&map, log_offt, &buf).await?;
			log_offt += 16;
		}

		Ok(())
	}

	/// Get or load the allocation map.
	async fn alloc_log(&self) -> Result<RefMut<'a, RangeSet<u64>>, Error<D>> {
		let data = self.fs.dir_data(self.id);

		// Check if the map has already been loaded.
		let data = match RefMut::filter_map(data, |data| data.alloc_map.as_mut()) {
			Ok(r) => return Ok(r),
			Err(data) => data,
		};

		let alloc_log_base = data.alloc_log_base();

		drop(data);

		// Load the allocation log
		let mut m = RangeSet::new();
		let l = self.fs.length(self.id).await?;

		for offt in (alloc_log_base..l).step_by(16) {
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

		Ok(RefMut::map(self.fs.dir_data(self.id), |data| {
			data.alloc_map.insert(m)
		}))
	}
}

impl<'a, D: Dev> DirRef<'a, D> {
	/// Create a new directory.
	pub(crate) async fn new(
		fs: &'a Nrfs<D>,
		parent_id: u64,
		parent_index: u32,
		options: &DirOptions,
	) -> Result<DirRef<'a, D>, Error<D>> {
		// Initialize data.
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
		let mut data = DirData {
			reference_count: 1,
			children: Default::default(),
			parent_id,
			parent_index,
			header_len8: ((header_len + 7) / 8).try_into().unwrap(),
			entry_len8: ((entry_len + 7) / 8).try_into().unwrap(),
			hashmap_size_p2: options.capacity_p2,
			hash_key: options.hash_key,
			hash_algorithm: options.hash_algorithm,
			entry_count: 0,
			unix_offset,
			mtime_offset,
			alloc_map: Some(Default::default()),
		};

		// Create objects.
		let (slf_obj, heap_obj) = fs.storage.create_pair().await?;
		let slf_id = slf_obj.id();

		// Create hashmap
		Dir::new(fs, slf_id)
			.init_with_size(&slf_obj, options.capacity_p2)
			.await?;

		// Insert directory data & return reference.
		fs.data.borrow_mut().directories.insert(slf_id, data);
		Ok(Self { fs, id: slf_id })
	}

	/// Load an existing directory.
	pub(crate) async fn load(
		fs: &'a Nrfs<D>,
		parent_id: u64,
		parent_index: u32,
		id: u64,
	) -> Result<DirRef<'a, D>, Error<D>> {
		// Check if the directory is already present in the filesystem object.
		//
		// If so, just reference that and return.
		if let Some(dir) = fs.data.borrow_mut().directories.get_mut(&id) {
			dir.reference_count += 1;
			return Ok(DirRef { fs, id });
		}

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

		let data = DirData {
			reference_count: 1,
			children: Default::default(),
			parent_id,
			parent_index,
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
		};

		// Insert directory data & return reference.
		fs.data.borrow_mut().directories.insert(id, data);
		Ok(Self { fs, id })
	}

	/// Create a new file.
	///
	/// This fails if an entry with the given name already exists.
	pub async fn create_file(
		&self,
		name: &Name,
		ext: &Extensions,
	) -> Result<Option<FileRef<'a, D>>, Error<D>> {
		let e = NewEntry { name, ty: Type::EmbedFile { offset: 0, length: 0 } };
		let index = self.dir().insert(e, ext).await?;
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
	) -> Result<Option<DirRef<'a, D>>, Error<D>> {
		// Try to insert stub entry
		let e = NewEntry { name, ty: Type::Dir { id: u64::MAX } };
		let Some(index) = self.dir().insert(e, ext).await? else { return Ok(None) };
		// Create new directory with stub index (u32::MAX).
		let d = DirRef::new(self.fs, self.id, u32::MAX, &options).await?;
		// Fixup ID in entry.
		self.dir().set_ty(index, Type::Dir { id: d.id }).await?;
		// Done!
		Ok(Some(d))
	}

	/// Create a new symbolic link.
	///
	/// This fails if an entry with the given name already exists.
	pub async fn create_sym(
		&self,
		name: &Name,
		ext: &Extensions,
	) -> Result<Option<SymRef<'a, D>>, Error<D>> {
		let e = NewEntry { name, ty: Type::EmbedSym { offset: 0, length: 0 } };
		let index = self.dir().insert(e, ext).await?;
		Ok(index.map(|i| SymRef::from_embed(&self.dir(), 0, 0, i)))
	}

	pub async fn next_from(
		&self,
		mut index: u32,
	) -> Result<Option<(Entry<'a, D>, Option<u32>)>, Error<D>> {
		while u64::from(index) < self.fs.dir_data(self.id).capacity() {
			// Get standard info
			let entry = self.dir().hashmap().await?.get(index).await?;

			if entry.ty == 0 {
				// Is empty, so skip
				index += 1;
				continue;
			}

			// Get extension info
			let entry = Entry::new(&self.dir(), index, &entry).await?;
			return Ok(Some((entry, index.checked_add(1))));
		}
		Ok(None)
	}

	/// Find an entry with the given name.
	pub async fn find(&self, name: &Name) -> Result<Option<Entry<'a, D>>, Error<D>> {
		let dir = self.dir();
		if let Some((index, entry)) = dir.hashmap().await?.find_index(name).await? {
			Ok(Some(Entry::new(&dir, index, &entry).await?))
		} else {
			Ok(None)
		}
	}

	/// Get the amount of entries in this directory.
	async fn len(&self) -> u32 {
		self.fs.dir_data(self.id).entry_count
	}

	/// Create a [`Dir`] helper structure.
	fn dir(&self) -> Dir<'a, D> {
		Dir::new(self.fs, self.id)
	}

	/// Destroy this dictionary.
	pub async fn destroy(self) -> Result<(), Error<D>> {
		todo!();
	}
}

/// A single entry in a directory.
#[derive(Debug)]
pub enum Entry<'a, D: Dev> {
	Dir(DirRef<'a, D>),
	File(FileRef<'a, D>),
	Sym(SymRef<'a, D>),
	Unknown(UnknownRef<'a, D>),
}

impl<'a, D: Dev> Entry<'a, D> {
	/// Construct an entry from raw entry data and the corresponding directory.
	async fn new(dir: &Dir<'a, D>, index: u32, entry: &RawEntry) -> Result<Entry<'a, D>, Error<D>> {
		Ok(match entry.ty() {
			Ok(Type::File { id }) => Self::File(FileRef::from_obj(dir, id, index)),
			Ok(Type::Sym { id }) => Self::Sym(SymRef::from_obj(dir, id, index)),
			Ok(Type::EmbedFile { offset, length }) => {
				Self::File(FileRef::from_embed(dir, offset, length, index))
			}
			Ok(Type::EmbedSym { offset, length }) => {
				Self::Sym(SymRef::from_embed(dir, offset, length, index))
			}
			Ok(Type::Dir { id }) => Self::Dir(DirRef::load(dir.fs, dir.id, index, id).await?),
			Err(_) => Self::Unknown(UnknownRef::new(dir, index)),
		})
	}

	/// Destroy this entry and the data it points to.
	///
	/// If the type is [`Self::Unknown`],
	/// this function will fail and `false` is returned.
	pub async fn destroy(self) -> Result<bool, Error<D>> {
		match self {
			Self::Dir(e) => e.destroy().await,
			Self::File(e) => e.destroy().await,
			Self::Sym(e) => e.destroy().await,
			Self::Unknown(_) => return Ok(false),
		}?;
		Ok(true)
	}
}

/// Get entry data.
#[derive(Debug)]
pub struct EntryData {
	/// The key of this entry.
	pub key: Box<Name>,
	/// The type of this entry.
	pub ty: (), // FIXME
	/// `unix` extension data, if present.
	pub ext_unix: Option<ext::unix::Entry>,
	/// `mtime` extension data, if present.
	pub ext_mtime: Option<ext::unix::Entry>,
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

/// A file or directory that is a child of another directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Child {
	File(Idx),
	Dir(u64),
}
