use {
	super::{
		ext, Dir, Error, Name, Nrfs, Type, TY_DIR, TY_EMBED_FILE, TY_EMBED_SYM, TY_FILE, TY_NONE,
		TY_SYM,
	},
	crate::{read_exact, write_all},
	nros::{Dev, Tree},
};

pub(super) struct HashMap<'a, D: Dev> {
	/// The filesystem containing the hashmap's data.
	fs: &'a Nrfs<D>,
	/// The ID of the directory.
	///
	/// Used for accessing the corresponding [`DirData`].
	dir_id: u64,
	/// The tree containing the hashmap's data.
	///
	/// The ID of this tree may be different from `Self::dir_id` if this hashmap is a new map
	/// created in a resize.
	pub(super) map: Tree<'a, D>,
	/// Mask applied to indices while iterating.
	mask: u32,
}

impl<'a, D: Dev> HashMap<'a, D> {
	/// Create a [`HashMap`] helper structure.
	pub fn new(dir: &Dir<'a, D>, map: Tree<'a, D>, size_p2: u8) -> Self {
		let &Dir { fs, id: dir_id } = dir;
		Self { fs, dir_id, map, mask: 1u32.wrapping_shl(size_p2.into()).wrapping_sub(1) }
	}

	/// Remove an entry.
	///
	/// This does not free heap data!
	pub async fn remove_at(&self, mut index: u32) -> Result<(), Error<D>> {
		let hash_algorithm = self.fs.dir_data(self.dir_id).hash_algorithm;

		match hash_algorithm {
			// shift entries if using Robin Hood hashing
			HashAlgorithm::SipHasher13 => {
				let mask = self.mask;
				let calc_psl = |i: u32, h| i.wrapping_sub(h) & mask;

				loop {
					let e = self.get((index + 1) & mask).await?;
					// No need to shift anything else
					if e.ty == 0 || calc_psl(index + 1, e.hash) == 0 {
						break;
					}
					self.set(&e).await?;
					index += 1;
					index &= mask;
				}
			}
		}

		// Mark entry as empty
		let offt = self.fs.dir_data(self.dir_id).get_offset(index);
		write_all(&self.map, offt + 7, &[TY_NONE]).await?;

		Ok(())
	}

	/// Find an entry with the given name.
	pub async fn find_index(&self, name: &Name) -> Result<Option<RawEntry>, Error<D>> {
		let data = self.fs.dir_data(self.dir_id);
		let hash = data.hash(name);
		let mut index = hash & self.mask;
		let hash_algorithm = data.hash_algorithm;
		let dir = Dir::new(self.fs, self.dir_id);
		drop(data);

		match hash_algorithm {
			HashAlgorithm::SipHasher13 => loop {
				let e = self.get(index).await?;
				if e.ty == 0 {
					return Ok(None);
				}
				if dir.compare_names(&e, name, hash).await? {
					break Ok(Some(e));
				}
				index += 1;
				index &= self.mask;
			},
		}
	}

	/// Get the raw standard info for an entry.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	pub async fn get(&self, index: u32) -> Result<RawEntry, Error<D>> {
		// Get data necessary to extract entry.
		let data = self.fs.dir_data(self.dir_id);
		let offt = data.get_offset(index);
		let len = data.entry_size();
		let unix_offset = data.unix_offset;
		let mtime_offset = data.mtime_offset;
		drop(data);

		// Read entry.
		let mut buf = vec![0; len.into()];
		read_exact(&self.map, offt, &mut buf).await?;

		// Get ty, key len, key offset
		let &[ty, key_len, a, b, c, d, e, f, ref buf @ ..] = &buf[..] else { todo!() };
		let key_offset = u64::from_le_bytes([a, b, c, d, e, f, 0, 0]);

		// Get hash, padding
		let &[a, b, c, d, e, f, g, h, ref buf @ ..] = buf else { todo!() };
		let hash = u32::from_le_bytes([a, b, c, d]);
		let padding = [e, f, g, h];

		// Get ID or (offset, len)
		let &[a, b, c, d, e, f, g, h, ref buf @ ..] = buf else { todo!() };
		let id_or_offset = u64::from_le_bytes([a, b, c, d, e, f, g, h]);

		// Get extensions

		// Get unix info
		let ext_unix = unix_offset
			.map(usize::from)
			.map(|o| ext::unix::Entry::from_raw(buf[o..o + 8].try_into().unwrap()));

		// Get mtime info
		let ext_mtime = mtime_offset
			.map(usize::from)
			.map(|o| ext::mtime::Entry::from_raw(buf[o..o + 8].try_into().unwrap()));

		Ok(RawEntry {
			ty,
			key_len,
			key_offset,
			hash,
			padding,

			id_or_offset,

			ext_unix,
			ext_mtime,

			index,
		})
	}

	/// Set the raw standard info for an entry.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	pub async fn set(&self, entry: &RawEntry) -> Result<(), Error<D>> {
		// Get data necessary to write entry.
		let data = self.fs.dir_data(self.dir_id);
		let offt = data.get_offset(entry.index);
		let len = data.entry_size();
		let unix_offset = data.unix_offset;
		let mtime_offset = data.mtime_offset;
		drop(data);

		// Serialize entry.
		let mut buf = vec![0; len.into()];

		// Set ty, key len, key offset
		buf[..8].copy_from_slice(&entry.key_offset.to_le_bytes());
		buf[0] = entry.key_len;
		buf[1] = entry.ty;

		// Set hash, padding
		buf[8..12].copy_from_slice(&entry.hash.to_le_bytes());
		buf[12..16].copy_from_slice(&entry.padding);

		// Set ID or (offset, len)
		buf[16..24].copy_from_slice(&entry.id_or_offset.to_le_bytes());

		// Set extensions

		// Set unix info
		let ext_unix = unix_offset
			.map(usize::from)
			.and_then(|o| entry.ext_unix.map(|e| (o, e)))
			.map(|(o, e)| buf[o..o + 8].copy_from_slice(&e.into_raw()));

		// Get mtime info
		let ext_mtime = mtime_offset
			.map(usize::from)
			.and_then(|o| entry.ext_unix.map(|e| (o, e)))
			.map(|(o, e)| buf[o..o + 8].copy_from_slice(&e.into_raw()));

		// Write out entry.
		write_all(&self.map, offt, &buf).await
	}

	/// Set arbitrary data.
	pub async fn set_raw(&self, index: u32, offset: u16, data: &[u8]) -> Result<(), Error<D>> {
		let offt = self.fs.dir_data(self.dir_id).get_offset(index) + u64::from(offset);
		write_all(&self.map, offt, data).await
	}

	pub async fn insert(
		&self,
		mut entry: RawEntry,
		name: Option<&Name>,
	) -> Result<Option<u32>, Error<D>> {
		let data = self.fs.dir_data(self.dir_id);

		// Update hash if a name was specified.
		name.map(|n| entry.hash = data.hash(n));
		entry.index = entry.hash & self.mask;
		let mut psl = 0u32;

		// If entry_index is None, we're still working with the to-be inserted entry.
		// If it is Some, the entry has already been inserted and we're only shifting entries.
		let mut entry_index = None;

		let calc_psl = |i: u32, h| i.wrapping_sub(h) & self.mask;

		async fn insert_entry<'a, D: Dev>(
			slf: &HashMap<'a, D>,
			entry_index: &mut Option<u32>,
			name: Option<&Name>,
			e: &mut RawEntry,
		) -> Result<u32, Error<D>> {
			// Store name if we're inserting the original entry.
			if let Some(name) = entry_index.is_none().then(|| name).flatten() {
				let dir = Dir::new(slf.fs, slf.dir_id);
				if name.len_u8() <= 14 {
					e.embed_key(name.as_ref());
				} else {
					e.key_offset = dir.alloc(name.len_u8().into()).await?;
					e.key_len = name.len_u8();
					dir.write_heap(e.key_offset, name).await?;
				}
			}

			// Store entry data at index.
			slf.set(&e).await?;

			// Save the index of the original entry,
			// otherwise just return the index of original entry.
			Ok(*entry_index.get_or_insert(e.index))
		}

		let hash_algorithm = data.hash_algorithm;
		let dir = Dir::new(self.fs, self.dir_id);
		drop(data);

		match hash_algorithm {
			HashAlgorithm::SipHasher13 => loop {
				// Insert with robin-hood hashing,
				// i.e. keep shifting entries until we find an empty one.

				// Note we're updating entry.index, so entry != e
				let e = self.get(entry.index).await?;

				// We found a free slot.
				if e.ty == 0 {
					let entry_index = insert_entry(self, &mut entry_index, name, &mut entry).await?;
					// Return the index of the entry we were supposed to insert.
					return Ok(Some(entry_index))
				}

				// If the entry has the same name as us, exit.
				if let Some(name) = name {
					// If entry_index is Some, we're shifting existing entries which should not
					// have conflicting names, so skip in that case.
					if entry_index.is_none() && dir.compare_names(&e, name, entry.hash).await? {
						return Ok(None);
					}
				}

				// Check if the PSL (Probe Sequence Length) is lower than that of ours
				// If yes, swap with it.
				if psl > calc_psl(entry.index, e.hash) {
					insert_entry(self, &mut entry_index, name, &mut entry).await?;
					entry = e;
				}

				entry.index += 1;
				entry.index &= self.mask;
				psl += 1;
			},
		}
	}
}

/// Raw entry data.
#[derive(Debug)]
pub(super) struct RawEntry {
	// Header
	/// Entry type.
	pub ty: u8,
	/// Key length.
	///
	/// If the length is `<= 14`, the key is embedded,
	/// i.e. it must be extracted from [`Self::key_offset`], [`Self::hash`] and [`Self::padding`].
	/// Otherwise it must be fetched from the heap.
	pub key_len: u8,
	/// The offset of the key, unless [`Self::key_len`] is `<= 14`.
	pub key_offset: u64,
	/// The hash of the key, unless [`Self::key_len`] is `<= 14`.
	///
	/// Used to avoid heap fetches when moving entries.
	pub hash: u32,
	/// Unused padding, unless [`Self::key_len`] is `<= 14`.
	pub padding: [u8; 4],

	// Regular data
	/// Entry regular data.
	///
	/// The meaning of the value depends on entry type.
	/// Use [`Self::ty`] to determine.
	pub id_or_offset: u64,

	// Extension data
	/// `unix` extension data.
	pub ext_unix: Option<ext::unix::Entry>,
	/// `mtime` extension data.
	pub ext_mtime: Option<ext::mtime::Entry>,

	// Other
	/// The index of the entry.
	pub index: u32,
}

impl RawEntry {
	pub fn ty(&self) -> Result<Type, u8> {
		match self.ty {
			TY_DIR => Ok(Type::Dir { id: self.id_or_offset }),
			TY_FILE => Ok(Type::File { id: self.id_or_offset }),
			TY_SYM => Ok(Type::Sym { id: self.id_or_offset }),
			TY_EMBED_FILE => Ok(Type::EmbedFile {
				offset: self.id_or_offset & 0xff_ffff,
				length: (self.id_or_offset >> 48) as _,
			}),
			TY_EMBED_SYM => Ok(Type::EmbedSym {
				offset: self.id_or_offset & 0xff_ffff,
				length: (self.id_or_offset >> 48) as _,
			}),
			n => Err(n),
		}
	}

	/// Get the key.
	///
	/// This may be embedded, giving the key directly,
	/// or it may be on the heap in which case another fetch is necessary.
	pub fn key(&self) -> RawEntryKey {
		if self.key_len <= 14 {
			let mut data = [0; 14];
			data[..6].copy_from_slice(&self.key_offset.to_le_bytes()[..6]);
			data[6..10].copy_from_slice(&self.hash.to_le_bytes());
			data[10..].copy_from_slice(&self.padding);
			RawEntryKey::Embed { data, len: self.key_len }
		} else {
			RawEntryKey::Heap { offset: self.key_offset, len: self.key_len }
		}
	}

	/// Embed key data.
	fn embed_key(&mut self, key: &[u8]) {
		let mut data = [0; 14];
		data[..key.len()].copy_from_slice(key);
		let [a, b, c, d, e, f, data @ ..] = data;
		self.key_offset = u64::from_le_bytes([a, b, c, d, e, f, 0, 0]);
		let [a, b, c, d, padding @ ..] = data;
		self.hash = u32::from_le_bytes([a, b, c, d]);
		self.padding = padding;
	}
}

/// Entry key, which may be embedded or on the heap.
#[derive(Debug)]
pub(super) enum RawEntryKey {
	Embed { data: [u8; 14], len: u8 },
	Heap { offset: u64, len: u8 },
}

#[derive(Clone, Copy, Default, Debug)]
pub enum HashAlgorithm {
	#[default]
	SipHasher13 = 1,
}
