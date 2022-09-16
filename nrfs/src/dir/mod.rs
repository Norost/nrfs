use {
	crate::{Error, Nrfs, Storage},
	core::{fmt, num::NonZeroU8},
	siphasher::sip::SipHasher13,
	rangemap::RangeSet,
};

pub mod ext;

// TODO determine a good load factor.
const MAX_LOAD_FACTOR_MILLI: u64 = 875;
const MIN_LOAD_FACTOR_MILLI: u64 = 500;

const TY_FILE: u8 = 1;
const TY_DIR: u8 = 2;
const TY_SYM: u8 = 3;

pub struct Dir {
	id: u64,
	hashmap_base: u16,
	hashmap_size_p2: u8,
	hash_keys: [u8; 16],
	hash_algorithm: HashAlgorithm,
	entry_size: u16,
	entry_count: u32,
	unix: Option<u16>,
	embedded: Option<(u16, ext::embedded::Dir)>,
	// Lazily load the allocation map to save time when only reading.
	alloc_map: Option<RangeSet<u64>>,
}

impl Dir {
	pub(crate) fn new<S>(fs: &mut Nrfs<S>, key: [u8; 16]) -> Result<Self, Error<S>>
	where
		S: Storage,
	{
		// Allocate map & storage
		let s = Self::new_map(fs, 3, 2, 0, &key)?;
		let alloc = fs.storage.new_object().map_err(Error::Nros)?;
		assert_eq!(s.id + 1, alloc);
		Ok(s)
	}

	/// Allocate a new hashmap object.
	fn new_map<S>(fs: &mut Nrfs<S>, header_len8: u8, entry_len8: u8, map_size_p2: u8, key: &[u8; 16]) -> Result<Self, Error<S>>
	where
		S: Storage,
	{
		let id = fs.storage.new_object().map_err(Error::Nros)?;
		let hashmap_base = u16::from(header_len8) * 8;
		let entry_size = u16::from(entry_len8) * 8;
		let s = Self {
			id,
			hashmap_base,
			hashmap_size_p2: map_size_p2,
			hash_keys: *key,
			hash_algorithm: HashAlgorithm::SipHasher13,
			entry_size,
			entry_count: 0,
			unix: None,
			embedded: None,
			alloc_map: Some(Default::default()),
		};
		let mut buf = [0; 24];
		buf[0] = header_len8;
		buf[1] = entry_len8;
		buf[2] = 1; // SipHasher13
		buf[3] = map_size_p2;
		buf[8..].copy_from_slice(key);
		fs.write_all(id, 0, &buf)?;
		// FIXME gap should be added automatically
		for i in 0..1 << map_size_p2 {
			fs.write_all(id, u64::from(hashmap_base) + i * u64::from(entry_size), &[0; 16])?;
		}
		Ok(s)
	}

	pub(crate) fn load<S>(fs: &mut Nrfs<S>, id: u64) -> Result<Self, Error<S>>
	where
		S: Storage,
	{
		// Get basic info
		let mut buf = [0; 24];
		fs.read_exact(id, 0, &mut buf)?;
		let [hlen, elen, hash_algorithm, hashmap_size_p2, a, b, c, d, hash_keys @ ..] = buf;
		let header_len = u16::from(hlen) * 8;
		let entry_size = u16::from(elen) * 8;
		let entry_count = u32::from_le_bytes([a, b, c, d]);

		// Get extensions
		let mut unix = None;
		let mut embedded = None;
		let mut offt = 0;
		while offt < header_len {
			let mut buf = [0; 4];
			fs.read_exact(id, offt.into(), &mut buf)?;
			let [name_len, data_len, entry_offset @ ..] = buf;
			let entry_offset = u16::from_le_bytes(entry_offset);
			let total_len = u16::from(name_len) + u16::from(data_len);
			let mut buf = [0; 510];
			fs.read_exact(id, 8 + u64::from(offt) + 4, &mut buf[..total_len.into()])?;
			let (name, data) = buf.split_at(name_len.into());
			match name {
				b"unix" => unix = Some(entry_offset),
				b"embedded" => {
					let f = |i| {
						data.get(i)
							.and_then(|n| NonZeroU8::new(*n))
							.ok_or(Error::CorruptExtension)
					};
					embedded = Some((
						entry_offset,
						ext::embedded::Dir { file_ty: f(0)?, sym_ty: f(1)? },
					));
				}
				_ => {}
			}
			offt += 4 + total_len;
		}

		Ok(Self {
			id,
			hashmap_base: header_len.into(),
			hashmap_size_p2,
			hash_algorithm: match hash_algorithm {
				1 => HashAlgorithm::SipHasher13,
				n => return Err(Error::UnknownHashAlgorithm(n)),
			},
			hash_keys,
			entry_size,
			entry_count,
			unix,
			embedded,
			alloc_map: None,
		})
	}

	pub fn next_from<S>(
		&self,
		fs: &mut Nrfs<S>,
		mut index: u32,
	) -> Result<Option<(Entry, Option<u32>)>, Error<S>>
	where
		S: Storage,
	{
		while u64::from(index) < self.capacity() {
			dbg!();
			// Get standard info
			let (e, offt) = self.get(fs, index)?;

			if e.ty == 0 {
				// Is empty, so skip
				index += 1;
				continue;
			}

			// Get key
			let mut key = [0; 255];
			let key = &mut key[..e.key_len.into()];
			self.read_heap(fs, e.key_offset, key)?;

			// Get extension info
			return self
				.get_ext(fs, index, e, key)
				.map(|e| Some((e, index.checked_add(1))));
		}
		Ok(None)
	}

	pub fn find<S>(&mut self, fs: &mut Nrfs<S>, name: &[u8]) -> Result<Option<Entry>, Error<S>>
	where
		S: Storage,
	{
		dbg!();
		u8::try_from(name.len()).map_err(|_| Error::NameTooLong)?;
		self.find_index(fs, name)?
			.map(|(i, e)| self.get_ext(fs, i, e, name))
			.transpose()
	}

	pub fn insert<S>(&mut self, fs: &mut Nrfs<S>, entry: NewEntry) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		dbg!();
		let key_len = u8::try_from(entry.name.len()).map_err(|_| Error::NameTooLong)?;

		// Check if we should grow the hashmap
		if self.should_grow() {
			self.grow(fs)?;
		}

		let mut index = self.hash(entry.name) & self.index_mask();
		loop {
			dbg!(self.entry_count, self.capacity());
			let (e, _) = self.get(fs, index)?;
			if e.ty == 0 {
				break;
			}
			index += 1;
			index &= self.index_mask();
		}

		let mut key = [0; 255];
		key[..entry.name.len()].copy_from_slice(entry.name);

		let e = Entry {
			location: match entry.data {
				Data::Object(id) => Location::Object { id },
				Data::Data(d) if d.len() <= 1024 => todo!("embed data"),
				Data::Data(d) => todo!("create object"),
			},
			ty: entry.ty,
			key,
			key_len,
			unix: None,
		};
		self.set_ext(fs, index, &e)?;
		self.set_entry_count(fs, self.entry_count + 1)
	}

	pub fn remove<S>(&mut self, fs: &mut Nrfs<S>, name: &[u8]) -> Result<bool, Error<S>>
	where
		S: Storage,
	{
		u8::try_from(name.len()).map_err(|_| Error::NameTooLong)?;

		let (i, e) = if let Some(r) = self.find_index(fs, name)? {
			r
		} else {
			return Ok(false);
		};
		if e.ty == TY_DIR {
			// Be careful not to leak objects
			todo!("remove dir");
		}
		let offt = self.get_offset(i);
		fs.write_all(self.id, offt, &[0])?;
		self.set_entry_count(fs, self.entry_count - 1)?;

		// Check if we should shrink the hashmap
		if self.should_shrink() {
			todo!()
		}
		Ok(true)
	}

	fn find_index<S>(
		&mut self,
		fs: &mut Nrfs<S>,
		name: &[u8],
	) -> Result<Option<(u32, RawEntry)>, Error<S>>
	where
		S: Storage,
	{
		dbg!();
		let mut index @ last_index = self.hash(name) & self.index_mask();
		loop {
			dbg!(index);
			let (e, _) = self.get(fs, index)?;
			dbg!();
			if e.ty == 0 {
				return Ok(None);
			}
			dbg!(e.id_or_offset, e.key_len, e.key_offset);
			if self.compare_names(fs, (e.key_len, e.key_offset), name)? {
				dbg!();
				break Ok(Some((index, e)));
			}
			dbg!();
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
				let mut h = SipHasher13::new_with_key(&self.hash_keys);
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
	fn get<S>(&self, fs: &mut Nrfs<S>, index: u32) -> Result<(RawEntry, u64), Error<S>>
	where
		S: Storage,
	{
		let offt = self.get_offset(index);
		dbg!(index, offt);
		let mut buf = [0; 16];
		fs.read_exact(self.id, offt, &mut buf)?;
		let [a, b, c, d, e, f, key_len, ty, id @ ..] = buf;
		let key_offset = u64::from_le_bytes([a, b, c, d, e, f, 0, 0]);
		let id_or_offset = u64::from_le_bytes(id);
		Ok((RawEntry { key_offset, key_len, id_or_offset, ty }, offt))
	}

	/// Get an entry with extension data.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	/// If the name is longer than 255 bytes.
	fn get_ext<S>(
		&self,
		fs: &mut Nrfs<S>,
		index: u32,
		entry: RawEntry,
		name: &[u8],
	) -> Result<Entry, Error<S>>
	where
		S: Storage,
	{
		dbg!(entry.id_or_offset);
		let offt = self.get_offset(index);

		// Get unix info
		let unix = self
			.unix
			.map(|o| {
				let mut buf = [0; 2];
				fs.read_exact(self.id, offt + u64::from(o), &mut buf)?;
				Ok(ext::unix::Entry { permissions: u16::from_le_bytes(buf) })
			})
			.transpose()?;

		// Get embedded info
		let embedded = self
			.embedded
			.as_ref()
			.map(|(o, _)| {
				let mut buf = [0; 2];
				fs.read_exact(self.id, offt + u64::from(*o), &mut buf)?;
				Ok(u16::from_le_bytes(buf))
			})
			.transpose()?;

		let mut key = [0; 255];
		key[..name.len()].copy_from_slice(name);
		let key_len = name.len().try_into().unwrap();

		let location = embedded.map_or(Location::Object { id: entry.id_or_offset }, |length| {
			Location::Embedded { offset: entry.id_or_offset, length }
		});
		let ty = match entry.ty {
			TY_FILE => Type::File,
			TY_DIR => Type::Directory,
			TY_SYM => Type::Symlink,
			n if Some(n) == self.embedded.as_ref().map(|d| d.1.file_ty.get()) => Type::File,
			n if Some(n) == self.embedded.as_ref().map(|d| d.1.sym_ty.get()) => Type::Symlink,
			n => Type::Unknown(n),
		};
		Ok(Entry { location, ty, key_len, key, unix })
	}

	/// Set the raw standard info for an entry.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	fn set<S>(&mut self, fs: &mut Nrfs<S>, index: u32, entry: RawEntry) -> Result<u64, Error<S>>
	where
		S: Storage,
	{
		let offt = self.get_offset(index);
		let mut buf = [0; 16];
		buf[..8].copy_from_slice(&entry.key_offset.to_le_bytes());
		buf[6] = entry.key_len;
		buf[7] = entry.ty;
		buf[8..].copy_from_slice(&entry.id_or_offset.to_le_bytes());
		fs.write_all(self.id, offt, &buf)?;
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
	fn set_ext<S>(&mut self, fs: &mut Nrfs<S>, index: u32, entry: &Entry) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		// Store key
		dbg!();
		let key_offset = self.alloc(fs, entry.key_len.into())?;
		fs.write_all(self.id + 1, key_offset, &entry.key[..entry.key_len.into()])?;
		dbg!();

		let (id_or_offset, embed) = match &entry.location {
			Location::Object { id } => (*id, None),
			Location::Embedded { offset, length } => (*offset, Some(*length)),
		};

		let ty = if embed.is_some() {
			let (_, d) = self.embedded.as_ref().unwrap();
			match entry.ty {
				Type::File => d.file_ty.get(),
				Type::Symlink => d.sym_ty.get(),
				_ => panic!("cannot be embedded"),
			}
		} else {
			match entry.ty {
				Type::File => TY_FILE,
				Type::Directory => TY_DIR,
				Type::Symlink => TY_SYM,
				Type::Unknown(n) => n,
			}
		};

		// Set entry itself
		let e = RawEntry { key_len: entry.key_len, key_offset, id_or_offset, ty };
		let offt = self.set(fs, index, e)?;

		// Set unix info
		if let Some(o) = self.unix {
			let u = entry.unix.as_ref().map_or(0, |u| u.permissions);
			fs.write_all(self.id, offt + u64::from(o), &u.to_le_bytes())?;
		}

		// Set embedded info
		if let (Some((o, _)), Some(l)) = (self.embedded.as_ref(), embed) {
			fs.write_all(self.id, offt + u64::from(*o), &l.to_le_bytes())?;
		}

		Ok(())
	}

	/// Determine the offset of an entry.
	///
	/// # Panics
	///
	/// If the index is out of range.
	fn get_offset(&self, index: u32) -> u64 {
		assert!(u64::from(index) < self.capacity(), "index out of range");
		u64::from(self.hashmap_base) + u64::from(index) * u64::from(self.entry_size)
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
	fn compare_names<S>(&self, fs: &mut Nrfs<S>, x: (u8, u64), y: &[u8]) -> Result<bool, Error<S>>
	where
		S: Storage,
	{
		if usize::from(x.0) != y.len() {
			return Ok(false);
		}
		let mut buf = [0; 255];
		fs.read_exact(self.id + 1, x.1, &mut buf[..y.len()])?;
		dbg!(String::from_utf8_lossy(&buf[..y.len()]));
		dbg!(String::from_utf8_lossy(&y[..y.len()]));
		Ok(&buf[..y.len()] == y)
	}

	/// Allocate heap space for arbitrary data.
	///
	/// The returned region is not readable until it is written to.
	fn alloc<S>(&mut self, fs: &mut Nrfs<S>, len: u64) -> Result<u64, Error<S>>
	where
		S: Storage,
	{
		let log = self.alloc_log(fs)?;
		dbg!(&log);
		for r in log.gaps(&(0..u64::MAX)) {
			if r.end - r.start >= len {
				log.insert(r.start..r.start + len);
				dbg!(fs.length(self.id)?);
				self.save_alloc_log(fs)?;
				dbg!(fs.length(self.id)?);
				return Ok(r.start);
			}
		}
		// This is unreachable in practice.
		unreachable!("all 2^64 bytes are allocated");
	}

	/// Write a full, minimized allocation log.
	fn save_alloc_log<S>(&mut self, fs: &mut Nrfs<S>) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		let id = self.id;
		let mut log_offt = self.alloc_log_base();
		let log = self.alloc_log(fs)?;
		for r in log.iter() {
			let mut buf = [0; 16];
			buf[..8].copy_from_slice(&r.start.to_le_bytes());
			buf[8..].copy_from_slice(&(r.end - r.start).to_le_bytes());
			fs.write_all(id, log_offt, &buf)?;
			log_offt += 16;
		}
		dbg!(log_offt);
		fs.truncate(self.id, log_offt)
	}

	/// Get or load the allocation map.
	fn alloc_log<'a, S>(&'a mut self, fs: &mut Nrfs<S>) -> Result<&'a mut RangeSet<u64>, Error<S>>
	where
		S: Storage,
	{
		// I'd use as_mut() but the borrow checker has a bug :(
		if self.alloc_map.is_some() {
			return Ok(self.alloc_map.as_mut().unwrap());
		}
		let mut m = RangeSet::new();
		let l = fs.length(self.id)?;
		for offt in (self.alloc_log_base()..l).step_by(16) {
			let mut buf = [0; 16];
			fs.read_exact(self.id, offt, &mut buf)?;
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

	/// The base address of the allocation log.
	fn alloc_log_base(&self) -> u64 {
		dbg!(u64::from(self.hashmap_base) + u64::from(self.entry_size) * self.capacity())
	}

	/// Read a heap value.
	fn read_heap<S>(&self, fs: &mut Nrfs<S>, offset: u64, buf: &mut [u8]) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		fs.read_exact(self.id + 1, offset, buf)
	}

	/// Write a heap value.
	fn write_heap<S>(&self, fs: &mut Nrfs<S>, offset: u64, data: &[u8]) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		fs.write_all(self.id + 1, offset, data)
	}

	/// Grow the hashmap
	fn grow<S>(&mut self, fs: &mut Nrfs<S>) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		dbg!();
		let new_map = Self::new_map(fs, 3, 2, self.hashmap_size_p2 + 1, &self.hash_keys)?;

		// Copy entries
		for index in 0..self.capacity() {
			let mut buf = [0; 16];
			fs.read_exact(self.id, 24 + 16 * u64::from(index), &mut buf)?; 
			let [a, b, c, d, e, f, key_len, ty, ..] = buf;
			if ty == 0 {
				continue;
			}

			let key_offset = u64::from_le_bytes([a, b, c, d, e, f, 0, 0]);
			let mut key = [0; 255];
			fs.read_exact(self.id + 1, key_offset, &mut key[..key_len.into()])?;

			let mut i = self.hash(&key[..key_len.into()]);
			println!("> {:08x} - {:?}", i, String::from_utf8_lossy(&key[..key_len.into()]));

			loop {
				let mut c = [0];
				let new_i = i & new_map.index_mask();
				fs.read_exact(new_map.id, 24 + 16 * u64::from(new_i) + 7, &mut c)?; 
				if c[0] == 0 {
					fs.write_all(new_map.id, 24 + 16 * u64::from(new_i), &buf)?;
					break;
				}
				i += 1;
			}
		}

		// Copy alloc log
		let old_base = self.alloc_log_base();
		let new_base = new_map.alloc_log_base();
		let old_end = fs.length(self.id)?;
		for offt in (old_base..old_end).step_by(16) {
			let mut buf = [0; 16];
			fs.read_exact(self.id, offt, &mut buf)?; 
			fs.write_all(new_map.id, new_base - old_base + offt, &buf)?;
		}

		fs.storage.move_object(self.id, new_map.id).map_err(Error::Nros)?;
		dbg!(&self.alloc_map);
		*self = Self {
			id: self.id,
			entry_count: self.entry_count,
			alloc_map: core::mem::take(&mut self.alloc_map),
			..new_map
		};

		Ok(())
	}

	/// Update the entry count.
	fn set_entry_count<S>(&mut self, fs: &mut Nrfs<S>, count: u32) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		fs.write_all(self.id, 4, &count.to_le_bytes())?;
		self.entry_count = count;
		Ok(())
	}
}

struct RawEntry {
	key_offset: u64,
	key_len: u8,
	id_or_offset: u64,
	ty: u8,
}

pub struct Entry {
	location: Location,
	ty: Type,
	key_len: u8,
	key: [u8; 255],
	unix: Option<ext::unix::Entry>,
}

impl Entry {
	pub fn read<S>(&self, fs: &mut Nrfs<S>, offset: u64, buf: &mut [u8]) -> Result<usize, Error<S>>
	where
		S: Storage,
	{
		match &self.location {
			Location::Object { id } => fs.read(*id, offset, buf),
			_ => todo!(),
		}
	}
}

impl fmt::Debug for Entry {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Entry))
			.field("location", &self.location)
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
enum Location {
	Object { id: u64 },
	Embedded { offset: u64, length: u16 },
}

#[derive(Debug, Default)]
pub enum Type {
	#[default]
	File,
	Directory,
	Symlink,
	Unknown(u8),
}

#[derive(Default)]
pub struct NewEntry<'a> {
	pub data: Data<'a>,
	pub name: &'a [u8],
	pub ty: Type,
}

pub enum Data<'a> {
	Data(&'a [u8]),
	Object(u64),
}

impl Default for Data<'_> {
	fn default() -> Self {
		Self::Data(&[])
	}
}

enum HashAlgorithm {
	SipHasher13,
}
