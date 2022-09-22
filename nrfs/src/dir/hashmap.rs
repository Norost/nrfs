use {
	super::{ext, Dir, Error, Name, Storage, Type},
	siphasher::sip::SipHasher13,
};

pub(super) struct HashMap<'a, 'b, S: Storage> {
	dir: &'b mut Dir<'a, S>,
	id: u64,
	mask: u32,
}

impl<'a, 'b, S: Storage> HashMap<'a, 'b, S> {
	pub fn new(dir: &'b mut Dir<'a, S>, id: u64, size_p2: u8) -> Self {
		Self { dir, id, mask: 1u32.wrapping_shl(size_p2.into()).wrapping_sub(1) }
	}

	pub fn remove_at(&mut self, mut index: u32) -> Result<(), Error<S>> {
		match self.dir.hash_algorithm {
			// shift entries if using Robin Hood hashing
			HashAlgorithm::SipHasher13 => {
				let mask = self.mask;
				let calc_psl = |i: u32, h| i.wrapping_sub(h) & mask;

				loop {
					let e = self.get((index + 1) & mask)?;
					// No need to shift anything else
					if e.ty == 0 || calc_psl(index + 1, e.hash) == 0 {
						break;
					}
					let e = self.get_ext((index + 1) & mask, e)?;
					self.set_ext(index, &e)?;
					index += 1;
					index &= mask;
				}
			}
		}

		// Mark entry as empty
		let offt = self.get_offset(index);
		self.dir.fs.write_all(self.id, offt + 7, &[0])?;

		Ok(())
	}

	pub fn find_index(&mut self, name: &Name) -> Result<Option<(u32, RawEntry)>, Error<S>> {
		let mut index = self.hash(name) & self.mask;
		match self.dir.hash_algorithm {
			HashAlgorithm::SipHasher13 => loop {
				let e = self.get(index)?;
				if e.ty == 0 {
					return Ok(None);
				}
				if self.dir.compare_names((e.key_len, e.key_offset), name)? {
					break Ok(Some((index, e)));
				}
				index += 1;
				index &= self.mask;
			},
		}
	}

	fn hash(&self, key: &[u8]) -> u32 {
		use core::hash::Hasher as _;
		match self.dir.hash_algorithm {
			HashAlgorithm::SipHasher13 => {
				let mut h = SipHasher13::new_with_key(&self.dir.hash_key);
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
	pub fn get(&mut self, index: u32) -> Result<RawEntry, Error<S>> {
		let offt = self.get_offset(index);
		let mut buf = [0; 24];
		self.dir.fs.read_exact(self.id, offt, &mut buf)?;
		let [a, b, c, d, e, f, key_len, ty, buf @ ..] = buf;
		let key_offset = u64::from_le_bytes([a, b, c, d, e, f, 0, 0]);
		let [id @ .., a, b, c, d, _, _, _, _] = buf;
		let hash = u32::from_le_bytes([a, b, c, d]);
		let id_or_offset = u64::from_le_bytes(id);
		Ok(RawEntry { key_offset, key_len, id_or_offset, ty, hash, index })
	}

	/// Get an entry with extension data.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	pub fn get_ext(&mut self, index: u32, entry: RawEntry) -> Result<RawEntryExt, Error<S>> {
		let offt = self.get_offset(index);

		// Get unix info
		let unix = self
			.dir
			.unix_offset
			.map(|o| {
				let mut buf = [0; 8];
				self.dir
					.fs
					.read_exact(self.id, offt + u64::from(o), &mut buf)?;
				Ok(ext::unix::Entry::from_raw(buf))
			})
			.transpose()?;

		// Get mtime info
		let mtime = self
			.dir
			.mtime_offset
			.map(|o| {
				let mut buf = [0; 8];
				self.dir
					.fs
					.read_exact(self.id, offt + u64::from(o), &mut buf)?;
				Ok(ext::mtime::Entry::from_raw(buf))
			})
			.transpose()?;

		Ok(RawEntryExt { entry, unix, mtime })
	}

	/// Set the raw standard info for an entry.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	pub fn set(&mut self, index: u32, entry: &RawEntry) -> Result<u64, Error<S>> {
		let offt = self.get_offset(index);
		let mut buf = [0; 24];
		buf[..8].copy_from_slice(&entry.key_offset.to_le_bytes());
		buf[6] = entry.key_len;
		buf[7] = entry.ty;
		buf[8..16].copy_from_slice(&entry.id_or_offset.to_le_bytes());
		buf[16..20].copy_from_slice(&entry.hash.to_le_bytes());
		self.dir.fs.write_all(self.id, offt, &buf)?;
		Ok(offt)
	}

	/// Set an entry with key and extension data.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	pub fn set_ext(&mut self, index: u32, entry: &RawEntryExt) -> Result<(), Error<S>> {
		let offt = self.set(index, &entry.entry)?;

		// Set unix info
		if let Some(o) = self.dir.unix_offset {
			let u = entry.unix.map_or([0; 8], |u| u.into_raw());
			self.dir.fs.write_all(self.id, offt + u64::from(o), &u)?;
		}

		// Set mtime info
		if let Some(o) = self.dir.mtime_offset {
			let u = entry.mtime.map_or([0; 8], |u| u.into_raw());
			self.dir.fs.write_all(self.id, offt + u64::from(o), &u)?;
		}

		Ok(())
	}

	/// Determine the offset of an entry.
	///
	/// This does *not* check if the index is in range.
	fn get_offset(&self, index: u32) -> u64 {
		debug_assert!(index <= self.mask, "{} <= {}", index, self.mask);
		self.dir.hashmap_base() + u64::from(index) * self.dir.entry_size()
	}

	pub fn insert(
		&mut self,
		mut entry: RawEntryExt,
		name: Option<&Name>,
	) -> Result<Option<u32>, Error<S>> {
		let mask = self.mask;
		let mut index = entry.entry.hash & mask;
		let mut psl = 0u32;
		let mut entry_index = None;

		let calc_psl = |i: u32, h| i.wrapping_sub(h) & mask;

		let mut insert_entry = |slf: &mut Self, i, e: &mut RawEntryExt| {
			if let Some(name) = entry_index.is_none().then(|| name).flatten() {
				// Store name
				e.entry.key_offset = slf.dir.alloc(name.len_u8().into())?;
				e.entry.key_len = name.len_u8();
				slf.dir.write_heap(e.entry.key_offset, name)?;
			}

			e.entry.index = i;
			slf.set_ext(i, e)?;

			if entry_index.is_none() {
				entry_index = Some(i);
			}

			Ok(())
		};

		match self.dir.hash_algorithm {
			HashAlgorithm::SipHasher13 => loop {
				let e = self.get(index)?;

				// We found a free slot.
				if e.ty == 0 {
					insert_entry(self, index, &mut entry)?;
					break;
				}

				// If the entry has the same name as us, exit.
				if name.map_or(Ok(false), |n| {
					self.dir.compare_names((e.key_len, e.key_offset), n)
				})? {
					break;
				}

				// Check if the PSL (Probe Sequence Length) is lower than that of ours
				// If yes, swap with it.
				if psl > calc_psl(index, e.hash) {
					let e = self.get_ext(index, e)?;
					insert_entry(self, index, &mut entry)?;
					entry = e;
				}

				index += 1;
				index &= mask;
				psl += 1;
			},
		}
		Ok(entry_index)
	}
}

#[derive(Debug)]
pub(super) struct RawEntry {
	pub key_offset: u64,
	pub key_len: u8,
	pub id_or_offset: u64,
	pub index: u32,
	pub ty: u8,
	pub hash: u32,
}

#[derive(Debug)]
pub(super) struct RawEntryExt {
	pub entry: RawEntry,
	pub unix: Option<ext::unix::Entry>,
	pub mtime: Option<ext::mtime::Entry>,
}

#[derive(Clone, Copy, Default, Debug)]
pub enum HashAlgorithm {
	#[default]
	SipHasher13 = 1,
}
