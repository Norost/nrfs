use {
	crate::{Error, File, Name, Nrfs, Storage},
	core::{
		fmt,
		ops::{Deref, DerefMut},
	},
	rangemap::RangeSet,
	siphasher::sip::SipHasher13,
};

pub mod ext;

// TODO determine a good load factor.
const MAX_LOAD_FACTOR_MILLI: u64 = 875;
const MIN_LOAD_FACTOR_MILLI: u64 = 375;

const TY_FILE: u8 = 1;
const TY_DIR: u8 = 2;
const TY_SYM: u8 = 3;
const TY_EMBED_FILE: u8 = 4;
const TY_EMBED_SYM: u8 = 5;

pub struct Dir<'a, S: Storage> {
	pub(crate) fs: &'a mut Nrfs<S>,
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

impl<S: Storage> Deref for Dir<'_, S> {
	type Target = DirData;

	fn deref(&self) -> &Self::Target {
		&self.data
	}
}

impl<S: Storage> DerefMut for Dir<'_, S> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.data
	}
}

impl<'a, S: Storage> Dir<'a, S> {
	pub(crate) fn new(fs: &'a mut Nrfs<S>, options: &DirOptions) -> Result<Self, Error<S>> {
		let mut header_len = 24;
		let mut entry_len = 16;
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
		slf.id = slf.fs.storage.new_object_pair().map_err(Error::Nros)?;
		slf.init_with_size(slf.id, options.capacity_p2)?;
		Ok(slf)
	}

	/// Initialize a hashmap object with the given size.
	///
	/// This does not modify the current dir structure.
	fn init_with_size(&mut self, id: u64, map_size_p2: u8) -> Result<(), Error<S>> {
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
			.resize(id, self.hashmap_base() + self.entry_size() << map_size_p2)
			.map_err(Error::Nros)?;
		self.fs
			.write_all(id, 0, &buf[..usize::from(self.header_len8) * 8])?;

		Ok(())
	}

	pub(crate) fn load(fs: &'a mut Nrfs<S>, id: u64) -> Result<Self, Error<S>> {
		// Get basic info
		let mut buf = [0; 24];
		fs.read_exact(id, 0, &mut buf)?;
		let [header_len8, entry_len8, hash_algorithm, hashmap_size_p2, a, b, c, d, hash_key @ ..] =
			buf;
		let entry_count = u32::from_le_bytes([a, b, c, d]);

		// Get extensions
		let mut unix_offset = None;
		let mut mtime_offset = None;
		let mut offt = 24;
		while offt < u16::from(header_len8) * 8 {
			let mut buf = [0; 2];
			fs.read_exact(id, offt.into(), &mut buf)?;
			let [name_len, data_len] = buf;
			let total_len = u16::from(name_len) + u16::from(data_len);
			let mut buf = [0; 255 * 2];
			fs.read_exact(id, u64::from(offt) + 2, &mut buf[..total_len.into()])?;
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
	pub fn create_file<'b>(
		&'b mut self,
		name: &Name,
		ext: &Extensions,
	) -> Result<Option<File<'a, 'b, S>>, Error<S>> {
		let e = NewEntry { name, ty: Type::EmbedFile { offset: 0, length: 0 } };
		self.insert(e, ext)
			.map(|r| r.map(|i| File::from_embed(self, false, i, 0, 0)))
	}

	/// Create a new directory.
	///
	/// This fails if an entry with the given name already exists.
	pub fn create_dir<'s>(
		&'s mut self,
		name: &Name,
		options: &DirOptions,
		ext: &Extensions,
	) -> Result<Option<Dir<'s, S>>, Error<S>> {
		let d = Dir::new(self.fs, options)?.data;
		let e = NewEntry { name, ty: Type::Dir { id: d.id } };
		self.insert(e, ext)
			.map(|r| r.map(|_| Dir { fs: self.fs, data: d }))
	}

	/// Create a new symbolic link.
	///
	/// This fails if an entry with the given name already exists.
	pub fn create_sym<'b>(
		&'b mut self,
		name: &Name,
		ext: &Extensions,
	) -> Result<Option<File<'a, 'b, S>>, Error<S>> {
		let e = NewEntry { name, ty: Type::EmbedSym { offset: 0, length: 0 } };
		self.insert(e, ext)
			.map(|r| r.map(|i| File::from_embed(self, true, i, 0, 0)))
	}

	pub fn next_from<'b>(
		&'b mut self,
		mut index: u32,
	) -> Result<Option<(Entry<'a, 'b, S>, Option<u32>)>, Error<S>> {
		while u64::from(index) < self.capacity() {
			// Get standard info
			let e = self.get(index)?;

			if e.ty == 0 {
				// Is empty, so skip
				index += 1;
				continue;
			}

			// Get key
			let mut key = [0; 255];
			let key = &mut key[..e.key_len.into()];
			self.read_heap(e.key_offset, key)?;

			// Get extension info
			return self
				.get_ext(index, e, key)
				.map_err(|_| todo!())
				.map(|e| Some((e, index.checked_add(1))));
		}
		Ok(None)
	}

	pub fn find<'b>(&'b mut self, name: &Name) -> Result<Option<Entry<'a, 'b, S>>, Error<S>> {
		self.find_index(name)?
			.map(|(i, e)| self.get_ext(i, e, name))
			.transpose()
	}

	/// Try to insert a new entry.
	///
	/// Returns `true` if succesful, `false` if an entry with the same name already exists.
	fn insert(&mut self, entry: NewEntry<'_>, ext: &Extensions) -> Result<Option<u32>, Error<S>> {
		// Check if we should grow the hashmap
		if self.should_grow() {
			self.grow()?;
		}

		let mut index = self.hash(entry.name) & self.index_mask();
		loop {
			let e = self.get(index)?;
			if e.ty == 0 {
				break;
			}
			index += 1;
			index &= self.index_mask();
		}

		let mut key = [0; 255];
		key[..entry.name.len()].copy_from_slice(entry.name);

		// Store name
		let name_offset = self.alloc(entry.name.len_u8().into())?;
		self.write_heap(name_offset, entry.name)?;

		let e = RawEntry {
			key_offset: name_offset,
			key_len: entry.name.len_u8(),
			ty: entry.ty.to_ty(),
			id_or_offset: entry.ty.to_data(),
		};
		self.set_ext(index, &e, ext)?;
		self.set_entry_count(self.entry_count + 1)?;
		Ok(Some(index))
	}

	pub fn remove(&mut self, name: &Name) -> Result<bool, Error<S>> {
		let (i, e) = if let Some(r) = self.find_index(name)? {
			r
		} else {
			return Ok(false);
		};
		if e.ty == TY_DIR {
			// Be careful not to leak objects
			todo!("remove dir");
		}
		let offt = self.get_offset(i);
		// Set ty to 0
		self.fs.write_all(self.id, offt + 7, &[0])?;
		self.set_entry_count(self.entry_count - 1)?;

		// Check if we should shrink the hashmap
		if self.should_shrink() {
			self.shrink()?;
		}
		Ok(true)
	}

	fn find_index(&mut self, name: &[u8]) -> Result<Option<(u32, RawEntry)>, Error<S>> {
		let mut index @ last_index = self.hash(name) & self.index_mask();
		loop {
			let e = self.get(index)?;
			if e.ty == 0 {
				return Ok(None);
			}
			if self.compare_names((e.key_len, e.key_offset), name)? {
				break Ok(Some((index, e)));
			}
			index += 1;
			index &= self.index_mask();
			if index == last_index {
				return Ok(None);
			}
		}
	}

	fn hash(&self, key: &[u8]) -> u32 {
		use core::hash::Hasher as _;
		match self.hash_algorithm {
			HashAlgorithm::SipHasher13 => {
				let mut h = SipHasher13::new_with_key(&self.hash_key);
				h.write(key);
				h.finish() as _
			}
		}
	}

	/// Get the raw standard info for an entry.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	fn get(&mut self, index: u32) -> Result<RawEntry, Error<S>> {
		let offt = self.get_offset(index);
		let mut buf = [0; 16];
		self.fs.read_exact(self.id, offt, &mut buf)?;
		let [a, b, c, d, e, f, key_len, ty, id @ ..] = buf;
		let key_offset = u64::from_le_bytes([a, b, c, d, e, f, 0, 0]);
		let id_or_offset = u64::from_le_bytes(id);
		Ok(RawEntry { key_offset, key_len, id_or_offset, ty })
	}

	/// Get an entry with extension data.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	/// If the name is longer than 255 bytes.
	fn get_ext<'b>(
		&'b mut self,
		index: u32,
		entry: RawEntry,
		name: &[u8],
	) -> Result<Entry<'a, 'b, S>, Error<S>> {
		let offt = self.get_offset(index);

		// Get unix info
		let unix = self
			.unix_offset
			.map(|o| {
				let mut buf = [0; 8];
				self.fs.read_exact(self.id, offt + u64::from(o), &mut buf)?;
				Ok(ext::unix::Entry::from_raw(buf))
			})
			.transpose()?;

		// Get mtime info
		let mtime = self
			.mtime_offset
			.map(|o| {
				let mut buf = [0; 8];
				self.fs.read_exact(self.id, offt + u64::from(o), &mut buf)?;
				Ok(ext::mtime::Entry::from_raw(buf))
			})
			.transpose()?;

		let mut key = [0; 255];
		key[..name.len()].copy_from_slice(name);
		let key_len = name.len().try_into().unwrap();

		let ty = match entry.ty {
			TY_DIR => Ok(Type::Dir { id: entry.id_or_offset }),
			TY_FILE => Ok(Type::File { id: entry.id_or_offset }),
			TY_SYM => Ok(Type::Sym { id: entry.id_or_offset }),
			TY_EMBED_FILE => Ok(Type::EmbedFile {
				offset: entry.id_or_offset & 0xff_ffff,
				length: (entry.id_or_offset >> 48) as _,
			}),
			TY_EMBED_SYM => Ok(Type::EmbedSym {
				offset: entry.id_or_offset & 0xff_ffff,
				length: (entry.id_or_offset >> 48) as _,
			}),
			n => Err(n),
		};
		Ok(Entry { dir: self, index, ty, key_len, key, unix, mtime })
	}

	/// Set the type and offset of an entry.
	///
	/// The entry must not be empty, i.e. type is not 0.
	pub(crate) fn set_ty(&mut self, index: u32, ty: Type) -> Result<(), Error<S>> {
		let mut e = self.get(index)?;
		debug_assert!(e.ty != 0);
		e.ty = ty.to_ty();
		e.id_or_offset = ty.to_data();
		self.set(index, &e).map(|_: u64| ())
	}

	/// Set the raw standard info for an entry.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	fn set(&mut self, index: u32, entry: &RawEntry) -> Result<u64, Error<S>> {
		let offt = self.get_offset(index);
		let mut buf = [0; 16];
		buf[..8].copy_from_slice(&entry.key_offset.to_le_bytes());
		buf[6] = entry.key_len;
		buf[7] = entry.ty;
		buf[8..].copy_from_slice(&entry.id_or_offset.to_le_bytes());
		self.fs.write_all(self.id, offt, &buf)?;
		Ok(offt)
	}

	/// Set an entry with key and extension data.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	/// If the name is longer than 255 bytes.
	fn set_ext(&mut self, index: u32, entry: &RawEntry, ext: &Extensions) -> Result<(), Error<S>> {
		let offt = self.set(index, entry)?;

		// Set unix info
		if let Some(o) = self.unix_offset {
			let u = ext.unix.map_or([0; 8], |u| u.into_raw());
			self.fs.write_all(self.id, offt + u64::from(o), &u)?;
		}

		// Set mtime info
		if let Some(o) = self.mtime_offset {
			let u = ext.mtime.map_or([0; 8], |u| u.into_raw());
			self.fs.write_all(self.id, offt + u64::from(o), &u)?;
		}

		Ok(())
	}

	/// Determine the offset of an entry.
	///
	/// This does *not* check if the index is in range.
	fn get_offset(&self, index: u32) -> u64 {
		u64::from(self.header_len8) * 8 + u64::from(index) * u64::from(self.entry_len8) * 8
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
	fn compare_names(&mut self, x: (u8, u64), y: &[u8]) -> Result<bool, Error<S>> {
		if usize::from(x.0) != y.len() {
			return Ok(false);
		}
		let mut buf = [0; 255];
		self.fs.read_exact(self.id + 1, x.1, &mut buf[..y.len()])?;
		Ok(&buf[..y.len()] == y)
	}

	/// Allocate heap space for arbitrary data.
	///
	/// The returned region is not readable until it is written to.
	fn alloc(&mut self, len: u64) -> Result<u64, Error<S>> {
		let log = self.alloc_log()?;
		for r in log.gaps(&(0..u64::MAX)) {
			if r.end - r.start >= len {
				log.insert(r.start..r.start + len);
				self.save_alloc_log()?;
				return Ok(r.start);
			}
		}
		// This is unreachable in practice.
		unreachable!("all 2^64 bytes are allocated");
	}

	/// Write a full, minimized allocation log.
	fn save_alloc_log(&mut self) -> Result<(), Error<S>> {
		let id = self.id;
		let mut log_offt = self.alloc_log_base();
		// Avoid mutable borrow issues
		self.alloc_log()?;
		let log = self.data.alloc_map.as_mut().unwrap();
		self.fs
			.resize(id, log_offt + 16 * log.iter().size_hint().0 as u64)?;
		for r in log.iter() {
			let mut buf = [0; 16];
			buf[..8].copy_from_slice(&r.start.to_le_bytes());
			buf[8..].copy_from_slice(&(r.end - r.start).to_le_bytes());
			self.fs.write_all(id, log_offt, &buf)?;
			log_offt += 16;
		}
		self.fs.resize(self.id, log_offt)
	}

	/// Get or load the allocation map.
	fn alloc_log(&mut self) -> Result<&mut RangeSet<u64>, Error<S>> {
		// I'd use as_mut() but the borrow checker has a bug :(
		if self.alloc_map.is_some() {
			return Ok(self.alloc_map.as_mut().unwrap());
		}
		let mut m = RangeSet::new();
		let l = self.fs.length(self.id)?;
		for offt in (self.alloc_log_base()..l).step_by(16) {
			let mut buf = [0; 16];
			self.fs.read_exact(self.id, offt, &mut buf)?;
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
	pub(crate) fn read_heap(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), Error<S>> {
		self.fs.read_exact(self.id + 1, offset, buf)
	}

	/// Write a heap value.
	pub(crate) fn write_heap(&mut self, offset: u64, data: &[u8]) -> Result<(), Error<S>> {
		self.fs.write_grow(self.id + 1, offset, data)
	}

	/// Grow the hashmap
	fn grow(&mut self) -> Result<(), Error<S>> {
		debug_assert!(
			self.hashmap_size_p2 < 32,
			"hashmap is already at maximum size"
		);
		self.resize(self.hashmap_size_p2 + 1)
	}

	/// Shrink the hashmap.
	///
	/// There must be *at least* `capacity / 2 + 1` slots free,
	/// i.e. `entry_count < capacity / 2`.
	fn shrink(&mut self) -> Result<(), Error<S>> {
		debug_assert!(
			self.hashmap_size_p2 != 0,
			"hashmap is already at minimum size"
		);
		debug_assert!(
			u64::from(self.entry_count) < self.capacity() / 2,
			"not enough free slots"
		);
		self.resize(self.hashmap_size_p2 - 1)
	}

	/// Resize the hashmap
	fn resize(&mut self, new_size_p2: u8) -> Result<(), Error<S>> {
		// Since we're going to load the entire log we can as well minimize it.
		self.alloc_log()?;

		let new_map_id = self.fs.storage.new_object().map_err(Error::Nros)?;
		self.init_with_size(new_map_id, new_size_p2)?;
		let new_index_mask = 1u32.wrapping_shl(new_size_p2.into()).wrapping_sub(1);

		// Copy entries
		for index in (0..self.capacity()).map(|i| i as _) {
			let mut buf = [0; 64];
			self.fs.read_exact(
				self.id,
				self.get_offset(index),
				&mut buf[..self.entry_size() as _],
			)?;
			let [a, b, c, d, e, f, key_len, ty, ..] = buf;
			if ty == 0 {
				continue;
			}

			let key_offset = u64::from_le_bytes([a, b, c, d, e, f, 0, 0]);
			let mut key = [0; 255];
			self.read_heap(key_offset, &mut key[..key_len.into()])?;

			let mut i = self.hash(&key[..key_len.into()]);

			loop {
				let mut c = [0];
				let offt = self.get_offset(i & new_index_mask);
				self.fs.read_exact(new_map_id, offt + 7, &mut c)?;
				if c[0] == 0 {
					self.fs
						.write_all(new_map_id, offt, &buf[..self.entry_size() as _])?;
					break;
				}
				i += 1;
			}
		}

		// Replace old map
		self.fs
			.storage
			.move_object(self.id, new_map_id)
			.map_err(Error::Nros)?;
		self.hashmap_size_p2 = new_size_p2;
		self.save_alloc_log()
	}

	/// Update the entry count.
	fn set_entry_count(&mut self, count: u32) -> Result<(), Error<S>> {
		self.fs.write_all(self.id, 4, &count.to_le_bytes())?;
		self.entry_count = count;
		Ok(())
	}

	pub fn len(&self) -> u32 {
		self.entry_count
	}

	pub fn id(&self) -> u64 {
		self.id
	}
}

impl<S: Storage> fmt::Debug for Dir<'_, S>
where
	Nrfs<S>: fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Dir))
			.field("fs", &self.fs)
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
			.finish()
	}
}

impl DirData {}

struct RawEntry {
	key_offset: u64,
	key_len: u8,
	id_or_offset: u64,
	ty: u8,
}

pub struct Entry<'a, 'b, S: Storage> {
	dir: &'b mut Dir<'a, S>,
	index: u32,
	ty: Result<Type, u8>,
	key_len: u8,
	key: [u8; 255],
	unix: Option<ext::unix::Entry>,
	mtime: Option<ext::mtime::Entry>,
}

impl<'a, 'b, S: Storage> Entry<'a, 'b, S> {
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

	pub fn as_file(&mut self) -> Option<File<'a, '_, S>> {
		Some(match self.ty {
			Ok(Type::File { id }) => File::from_obj(self.dir, false, id, self.index),
			Ok(Type::EmbedFile { offset, length }) => {
				File::from_embed(self.dir, false, self.index, offset, length)
			}
			_ => return None,
		})
	}

	pub fn as_dir(&mut self) -> Option<Result<Dir<'_, S>, Error<S>>> {
		Some(match self.ty {
			Ok(Type::Dir { id }) => Dir::load(self.dir.fs, id),
			_ => return None,
		})
	}

	pub fn as_sym(&mut self) -> Option<File<'a, '_, S>> {
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

	pub fn ext_unix(&self) -> Option<&ext::unix::Entry> {
		self.unix.as_ref()
	}

	pub fn ext_mtime(&self) -> Option<&ext::mtime::Entry> {
		self.mtime.as_ref()
	}
}

impl<S: Storage + fmt::Debug> fmt::Debug for Entry<'_, '_, S> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Entry))
			.field("dir", self.dir)
			.field("index", &self.index)
			.field("ty", &self.ty)
			// TODO use Utf8Lossy when it is stable.
			.field(
				"key",
				&String::from_utf8_lossy(&self.key[..self.key_len.into()]),
			)
			.field("unix", &self.unix)
			.finish()
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

#[derive(Clone, Copy, Default, Debug)]
pub enum HashAlgorithm {
	#[default]
	SipHasher13 = 1,
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
