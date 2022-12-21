use {
	super::{
		ext, Dir, DirSize, Error, Name, Nrfs, Type, TY_DIR, TY_EMBED_FILE, TY_EMBED_SYM, TY_FILE,
		TY_NONE, TY_SYM,
	},
	crate::{read_exact, write_all},
	core::fmt,
	nros::{Dev, Tree},
	siphasher::sip::SipHasher13,
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
	pub fn new(dir: &Dir<'a, D>, map: Tree<'a, D>, size: DirSize) -> Self {
		let &Dir { fs, id: dir_id } = dir;
		Self {
			fs,
			dir_id,
			map,
			mask: 1u32.wrapping_shl(size.to_raw().into()).wrapping_sub(1),
		}
	}

	/// Remove an entry.
	///
	/// This does not free heap data nor does it remove the corresponding child!
	pub async fn remove_at(&self, mut index: u32) -> Result<(), Error<D>> {
		trace!("remove_at {:?}", index);
		let hasher = self.fs.dir_data(self.dir_id).hasher;

		match hasher {
			// shift entries if using Robin Hood hashing
			Hasher::SipHasher13(_) => {
				let calc_psl = |i: u32, h| i.wrapping_sub(h) & self.mask;

				// FIXME this can get stuck if the hashmap is malformed.
				//
				// We should inspect other such infinite loops.
				loop {
					// Get the next entry.
					let mut e = self.get((index + 1) & self.mask).await?;
					// No need to shift anything else because:
					// - if e.ty == 0, the next entry is empty.
					//   We will clear the current entry below anyways.
					// - if psl == 0 we don't want to shift it from its ideal position.
					if e.ty == 0 || calc_psl(index + 1, e.hash(&hasher)) == 0 {
						break;
					}

					// Copy the next entry over the curren entry and move on to the next.
					e.index = index;
					self.set(&e).await?;

					let mut data = self.fs.dir_data(self.dir_id);
					if let Some(child) = data.children.remove(&(index + 1)) {
						let _r = data.children.insert(index, child);
						assert!(_r.is_none(), "a child was already present");
						drop(data);
						child.header(self.fs).parent_index = index;
					} else {
						drop(data);
					}

					index += 1;
					index &= self.mask;
				}
			}
		}

		// Mark last shifted entry as empty
		let offt = self.fs.dir_data(self.dir_id).get_offset(index);
		write_all(&self.map, offt, &[TY_NONE]).await?;

		Ok(())
	}

	/// Find an entry with the given name.
	pub async fn find_index(&self, name: &Name) -> Result<Option<RawEntry>, Error<D>> {
		trace!("find_index {:?}", name);
		let hasher = self.fs.dir_data(self.dir_id).hasher;
		let hash = hasher.hash(name);
		let mut index = hash & self.mask;
		let dir = Dir::new(self.fs, self.dir_id);

		match hasher {
			Hasher::SipHasher13(_) => loop {
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
		trace!("get {:?}", index);
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

		// Get ty, key
		let (header, rem) = buf.split_array_ref::<16>();
		let &[ty, key_len, data @ ..] = header;
		let key = if key_len <= 14 {
			RawEntryKey::Embed { len: key_len, data }
		} else {
			let [a, b, c, d, e, f, data @ ..] = data;
			let (&hash, _) = data.split_array_ref::<4>();
			RawEntryKey::Heap {
				len: key_len,
				offset: u64::from_le_bytes([a, b, c, d, e, f, 0, 0]),
				hash: u32::from_le_bytes(hash),
			}
		};

		// Get ID or (offset, len)
		let (&id_or_offset, _) = rem.split_array_ref::<8>();
		let id_or_offset = u64::from_le_bytes(id_or_offset);

		// Get extensions

		// Get unix info
		let ext_unix = unix_offset
			.map(usize::from)
			.map(|o| ext::unix::Entry::from_raw(buf[o..o + 8].try_into().unwrap()));

		// Get mtime info
		let ext_mtime = mtime_offset
			.map(usize::from)
			.map(|o| ext::mtime::Entry::from_raw(buf[o..o + 8].try_into().unwrap()));

		Ok(RawEntry { ty, key, id_or_offset, ext_unix, ext_mtime, index })
	}

	/// Set the raw standard info for an entry.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	pub async fn set(&self, entry: &RawEntry) -> Result<(), Error<D>> {
		trace!("set {:?}", entry);
		debug_assert!(entry.ty != 0, "trying to set empty entry");
		// Get data necessary to write entry.
		let data = self.fs.dir_data(self.dir_id);
		let offt = data.get_offset(entry.index);
		let len = data.entry_size();
		let unix_offset = data.unix_offset;
		let mtime_offset = data.mtime_offset;
		drop(data);

		// Serialize entry.
		let mut buf = vec![0; len.into()];

		// Set ty, key
		buf[0] = entry.ty;
		match entry.key {
			RawEntryKey::Embed { len, data } => {
				buf[1] = len;
				buf[2..16].copy_from_slice(&data);
			}
			RawEntryKey::Heap { len, offset, hash } => {
				buf[1] = len;
				buf[2..8].copy_from_slice(&offset.to_le_bytes()[..6]);
				buf[8..12].copy_from_slice(&hash.to_le_bytes());
			}
		}

		// Set ID or (offset, len)
		buf[16..24].copy_from_slice(&entry.id_or_offset.to_le_bytes());

		// Set extensions

		// Set unix info
		unix_offset
			.map(usize::from)
			.and_then(|o| entry.ext_unix.map(|e| (o, e)))
			.map(|(o, e)| buf[o..o + 8].copy_from_slice(&e.into_raw()));

		// Get mtime info
		mtime_offset
			.map(usize::from)
			.and_then(|o| entry.ext_mtime.map(|e| (o, e)))
			.map(|(o, e)| buf[o..o + 8].copy_from_slice(&e.into_raw()));

		// Write out entry.
		write_all(&self.map, offt, &buf).await
	}

	/// Set arbitrary data.
	pub async fn set_raw(&self, index: u32, offset: u16, data: &[u8]) -> Result<(), Error<D>> {
		let offt = self.fs.dir_data(self.dir_id).get_offset(index) + u64::from(offset);
		write_all(&self.map, offt, data).await
	}

	/// Insert a new entry.
	///
	/// Optionally, a new name can be specified.
	/// The `key` present in `entry` will be ignored in that case.
	pub async fn insert(
		&self,
		mut entry: RawEntry,
		name: Option<&Name>,
	) -> Result<Option<u32>, Error<D>> {
		trace!("insert {:?} {:?}", (entry.index, &entry.key), &name);
		let data = self.fs.dir_data(self.dir_id);

		// Determine hash to use.
		let hash = if let Some(name) = name {
			// Update hash if a name was specified.
			data.hasher.hash(name)
		} else {
			// Take or calculate the hash from the existing key.
			entry.hash(&data.hasher)
		};
		let name = name.map(|name| (name, hash));

		// Update index
		entry.index = hash & self.mask;

		/// Set the key & insert the entry.
		async fn insert_entry<'a, D: Dev>(
			slf: &HashMap<'a, D>,
			name: Option<(&Name, u32)>,
			mut entry: RawEntry,
		) -> Result<u32, Error<D>> {
			trace!("insert::insert_entry {:?} {:?}", name, &entry);
			let dir = Dir::new(slf.fs, slf.dir_id);

			// Store name if necessary.
			if let Some((name, hash)) = name {
				entry.key = if name.len_u8() <= 14 {
					// Embed key
					let mut data = [0; 14];
					data[..name.len()].copy_from_slice(name.as_ref());
					RawEntryKey::Embed { len: name.len_u8(), data }
				} else {
					// Store key on heap
					let offset = dir.alloc(name.len_u8().into()).await?;
					dir.write_heap(offset, name).await?;
					RawEntryKey::Heap { len: name.len_u8(), offset, hash }
				};
			}

			// Store entry data at index.
			slf.set(&entry).await.map(|()| entry.index)
		}

		let hasher = data.hasher;
		let dir = Dir::new(self.fs, self.dir_id);
		drop(data);

		// Start with default PSL ("poorness")
		let mut entry_psl = 0u32;
		let calc_psl = |i: u32, h| i.wrapping_sub(h) & self.mask;

		match hasher {
			Hasher::SipHasher13(_) => {
				// Insert with robin-hood hashing,
				// - First, find an appropriate slot for the entry to be inserted.
				// - Then, shift every other entry forward until we hit an empty slot.

				// Insert current entry.
				let (entry_index, mut entry, mut child) = loop {
					let e = self.get(entry.index).await?;

					// We found a free slot.
					// Insert and return.
					if e.ty == 0 {
						return insert_entry(self, name, entry).await.map(Some);
					}

					// If the entry has the same name as us, exit.
					if let Some((name, hash)) = name {
						if dir.compare_names(&e, name, hash).await? {
							return Ok(None);
						}
					}

					// Check if the PSL (Probe Sequence Length) is lower than that of ours
					// If yes, swap with it and begin shifting forward.
					if entry_psl > calc_psl(entry.index, e.hash(&hasher)) {
						let index = insert_entry(self, name, entry).await?;
						let child = self.fs.dir_data(self.dir_id).children.remove(&index);
						break (index, e, child);
					}

					// Try next slot
					entry.index += 1;
					entry.index &= self.mask;
					entry_psl += 1;
				};

				// Shift all elements forward by one slot
				// until an empty slot is found.
				loop {
					entry.index += 1;
					entry.index &= self.mask;

					let e = self.get(entry.index).await?;

					// Insert unconditionally.
					let index = insert_entry(self, None, entry).await?;
					// Insert current child & take next child
					child = if let Some(c) = child {
						c.header(self.fs).parent_index = index;
						self.fs.dir_data(self.dir_id).children.insert(index, c)
					} else {
						self.fs.dir_data(self.dir_id).children.remove(&index)
					};

					// We found a free slot.
					// Free at last!
					if e.ty == 0 {
						debug_assert!(child.is_none(), "empty entry has child");
						return Ok(Some(entry_index));
					}

					// Continue with swapped out entry.
					entry = e;
				}
			}
		}
	}
}

/// Raw entry data.
#[derive(Clone, Debug)]
pub(super) struct RawEntry {
	// Header
	/// Entry type.
	pub ty: u8,
	/// Key data.
	///
	/// The variant depends on whether it is embedded or not.
	/// If the length is `<= 14` the key is embedded.
	/// If the length is `> 14` the key is stored on the heap.
	pub key: RawEntryKey,

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

	/// Take or calculate the hash from the existing key.
	fn hash(&self, hasher: &Hasher) -> u32 {
		match self.key {
			RawEntryKey::Embed { len, data: d } => hasher.hash(&d[..len.into()]),
			RawEntryKey::Heap { len: _, offset: _, hash } => hash,
		}
	}
}

/// Entry key, which may be embedded or on the heap.
#[derive(Clone)]
pub(super) enum RawEntryKey {
	Embed {
		/// The length of the key.
		///
		/// Must be `<= 14`.
		len: u8,
		/// The data of the key.
		data: [u8; 14],
	},
	Heap {
		/// The length of the key.
		///
		/// Must be `> 14`.
		len: u8,
		/// The offset of the key on the heap.
		offset: u64,
		/// The hash of the key, unless [`Self::key_len`] is `<= 14`.
		///
		/// Used to avoid heap fetches when moving or comparing entries.
		hash: u32,
	},
}

impl fmt::Debug for RawEntryKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			&Self::Embed { len, data } => f
				.debug_struct(stringify!(Embed))
				.field("len", &len)
				.field("data", &<&Name>::try_from(&data[..len.into()]).unwrap())
				.finish(),
			&Self::Heap { len, offset, hash } => f
				.debug_struct(stringify!(Heap))
				.field("len", &len)
				.field("offset", &offset)
				.field("hash", &format_args!("{:#10x}", &hash))
				.finish(),
		}
	}
}

/// Hasher helper structure.
///
/// Avoids the need to borrow `DirData` redundantly.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(any(test, fuzzing), derive(arbitrary::Arbitrary))]
pub enum Hasher {
	SipHasher13([u8; 16]),
}

impl Hasher {
	/// Turn this hasher into raw components for storage.
	///
	/// The first element represents the type,
	/// the second element represents the key.
	pub fn to_raw(self) -> (u8, [u8; 16]) {
		match self {
			Self::SipHasher13(h) => (1, h),
		}
	}

	/// Create a hasher from raw components.
	///
	/// Fails if the hasher type is unknown.
	pub fn from_raw(ty: u8, key: &[u8; 16]) -> Option<Self> {
		Some(match ty {
			1 => Self::SipHasher13(*key),
			_ => return None,
		})
	}

	/// Hash an arbitrary-sized key.
	fn hash(&self, data: &[u8]) -> u32 {
		use core::hash::Hasher;
		match self {
			Self::SipHasher13(key) => {
				let mut h = SipHasher13::new_with_key(key);
				h.write(data);
				h.finish() as _
			}
		}
	}
}
