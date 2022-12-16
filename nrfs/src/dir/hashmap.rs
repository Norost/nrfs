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
					let e = self.get_ext((index + 1) & mask, e).await?;
					self.set_ext(index, &e).await?;
					index += 1;
					index &= mask;
				}
			}
		}

		// Mark entry as empty
		let offt = self.get_offset(index);
		write_all(&self.map, offt + 7, &[TY_NONE]).await?;

		Ok(())
	}

	/// Find an entry with the given name.
	///
	/// Returns the index and info located in the entry itself.
	pub async fn find_index(&self, name: &Name) -> Result<Option<(u32, RawEntry)>, Error<D>> {
		let data = self.fs.dir_data(self.dir_id);
		let mut index = data.hash(name) & self.mask;
		let hash_algorithm = data.hash_algorithm;
		let dir = Dir::new(self.fs, self.dir_id);
		drop(data);

		match hash_algorithm {
			HashAlgorithm::SipHasher13 => loop {
				let e = self.get(index).await?;
				if e.ty == 0 {
					return Ok(None);
				}
				if dir.compare_names((e.key_len, e.key_offset), name).await? {
					break Ok(Some((index, e)));
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
		let offt = self.get_offset(index);
		let mut buf = [0; 24];
		read_exact(&self.map, offt, &mut buf).await?;
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
	pub async fn get_ext(&self, index: u32, entry: RawEntry) -> Result<RawEntryExt, Error<D>> {
		let offt = self.get_offset(index);

		let (unix_offset, mtime_offset) = {
			let data = self.fs.dir_data(self.dir_id);
			(data.unix_offset, data.mtime_offset)
		};

		// Get unix info
		let unix = if let Some(o) = unix_offset {
			let mut buf = [0; 8];
			read_exact(&self.map, offt + u64::from(o), &mut buf).await?;
			Some(ext::unix::Entry::from_raw(buf))
		} else {
			None
		};

		// Get mtime info
		let mtime = if let Some(o) = mtime_offset {
			let mut buf = [0; 8];
			read_exact(&self.map, offt + u64::from(o), &mut buf).await?;
			Some(ext::mtime::Entry::from_raw(buf))
		} else {
			None
		};

		Ok(RawEntryExt { entry, unix, mtime })
	}

	/// Set the raw standard info for an entry.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	pub async fn set(&self, index: u32, entry: &RawEntry) -> Result<u64, Error<D>> {
		let offt = self.get_offset(index);
		let mut buf = [0; 24];
		buf[..8].copy_from_slice(&entry.key_offset.to_le_bytes());
		buf[6] = entry.key_len;
		buf[7] = entry.ty;
		buf[8..16].copy_from_slice(&entry.id_or_offset.to_le_bytes());
		buf[16..20].copy_from_slice(&entry.hash.to_le_bytes());
		write_all(&self.map, offt, &buf).await?;
		Ok(offt)
	}

	/// Set an entry with key and extension data.
	///
	/// This does not check if the entry is empty or not.
	///
	/// # Panics
	///
	/// If the index is out of range.
	pub async fn set_ext(&self, index: u32, entry: &RawEntryExt) -> Result<(), Error<D>> {
		let offt = self.set(index, &entry.entry).await?;

		let (unix_offset, mtime_offset) = {
			let data = self.fs.dir_data(self.dir_id);
			(data.unix_offset, data.mtime_offset)
		};

		// Set unix info
		if let Some(o) = unix_offset {
			let u = entry.unix.map_or([0; 8], |u| u.into_raw());
			write_all(&self.map, offt + u64::from(o), &u).await?;
		}

		// Set mtime info
		if let Some(o) = mtime_offset {
			let u = entry.mtime.map_or([0; 8], |u| u.into_raw());
			write_all(&self.map, offt + u64::from(o), &u).await?;
		}

		Ok(())
	}

	/// Set arbitrary data.
	pub async fn set_raw(&self, index: u32, offset: u16, data: &[u8]) -> Result<(), Error<D>> {
		let o = self.get_offset(index) + u64::from(offset);
		write_all(&self.map, o, data).await
	}

	/// Determine the offset of an entry.
	///
	/// This does *not* check if the index is in range.
	fn get_offset(&self, index: u32) -> u64 {
		let data = self.fs.dir_data(self.dir_id);
		debug_assert!(index <= self.mask, "{} <= {}", index, self.mask);
		data.hashmap_base() + u64::from(index) * data.entry_size()
	}

	pub async fn insert(
		&self,
		mut entry: RawEntryExt,
		name: Option<&Name>,
	) -> Result<Option<u32>, Error<D>> {
		let data = self.fs.dir_data(self.dir_id);

		let mask = self.mask;
		name.map(|n| entry.entry.hash = data.hash(n));
		let mut index = entry.entry.hash & mask;
		let mut psl = 0u32;
		let mut entry_index = None;

		let calc_psl = |i: u32, h| i.wrapping_sub(h) & mask;

		async fn insert_entry<'a, D: Dev>(
			slf: &HashMap<'a, D>,
			entry_index: &mut Option<u32>,
			name: Option<&Name>,
			i: u32,
			e: &mut RawEntryExt,
		) -> Result<(), Error<D>> {
			if let Some(name) = entry_index.is_none().then(|| name).flatten() {
				// Store name
				let dir = Dir::new(slf.fs, slf.dir_id);
				e.entry.key_offset = dir.alloc(name.len_u8().into()).await?;
				e.entry.key_len = name.len_u8();
				dir.write_heap(e.entry.key_offset, name).await?;
			}

			e.entry.index = i;
			slf.set_ext(i, e).await?;

			if entry_index.is_none() {
				*entry_index = Some(i);
			}

			Ok(())
		}

		let hash_algorithm = data.hash_algorithm;
		let dir = Dir::new(self.fs, self.dir_id);
		drop(data);

		match hash_algorithm {
			HashAlgorithm::SipHasher13 => loop {
				let e = self.get(index).await?;

				// We found a free slot.
				if e.ty == 0 {
					insert_entry(self, &mut entry_index, name, index, &mut entry).await?;
					break;
				}

				// If the entry has the same name as us, exit.
				if let Some(n) = name {
					if dir.compare_names((e.key_len, e.key_offset), n).await? {
						break;
					}
				}

				// Check if the PSL (Probe Sequence Length) is lower than that of ours
				// If yes, swap with it.
				if psl > calc_psl(index, e.hash) {
					let e = self.get_ext(index, e).await?;
					insert_entry(self, &mut entry_index, name, index, &mut entry).await?;
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
