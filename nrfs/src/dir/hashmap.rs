use {
	super::{Dir, DirSize, Error, Hasher, Key, Name, Nrfs},
	crate::{read_exact, write_all, DirData},
	core::cell::RefMut,
	nros::{Dev, Tree},
	rangemap::RangeSet,
};

/// The size of a single hashmap entry.
const ENTRY_SIZE: u16 = 32;

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
	/// Size of this hashmap
	size: DirSize,
	/// The [`Hasher`] used to index this hashmap.
	hasher: Hasher,
}

impl<'a, D: Dev> HashMap<'a, D> {
	/// Create a [`HashMap`] helper structure.
	pub fn new(dir: &Dir<'a, D>, data: &DirData, map: Tree<'a, D>, size: DirSize) -> Self {
		Self { fs: dir.fs, dir_id: dir.id, map, size, hasher: data.hasher }
	}

	/// Initialize a new hashmap structure.
	///
	/// This will create a new object.
	pub async fn create(dir: &Dir<'a, D>, size: DirSize) -> Result<HashMap<'a, D>, Error<D>> {
		trace!("hashmap::create {:?}", size);
		// Get entry size and current alloc log.
		let data = dir.fs.dir_data(dir.id);
		let hasher = data.hasher;
		drop(data);
		let log = dir.heap_alloc_log().await?.clone();

		// Create object.
		let map = dir.fs.storage.create().await?;
		map.resize((1u64 << size) * u64::from(ENTRY_SIZE)).await?;

		// Save allocation log.
		let s = Self { fs: dir.fs, dir_id: dir.id, map, size, hasher };
		s.save_heap_alloc_log(&log).await?;

		// Done
		Ok(s)
	}

	/// Remove an entry.
	///
	/// This does not free heap data!
	pub async fn remove_at(&self, mut index: u32) -> Result<(), Error<D>> {
		trace!("remove_at {:?}", index);
		match self.hasher {
			// shift entries if using Robin Hood hashing
			Hasher::SipHasher13(_) => {
				let calc_psl = |i: u32, h| i.wrapping_sub(h as u32) & self.mask();

				// FIXME this can get stuck if the hashmap is malformed.
				//
				// We should inspect other such infinite loops.
				loop {
					// Get the next entry.
					let next_index = (index + 1) & self.mask();
					let mut e = self.get(next_index).await?;
					// No need to shift anything else because:
					// - if e.ty == 0, the next entry is empty.
					//   We will clear the current entry below anyways.
					// - if psl == 0 we don't want to shift it from its ideal position.
					if e.hash(&self.hasher)
						.map_or(true, |hash| calc_psl(index + 1, hash) == 0)
					{
						break;
					}

					// Copy the next entry over the curren entry and move on to the next.
					e.index = index;
					self.set(&e).await?;

					index += 1;
					index &= self.mask();
				}
			}
		}

		// Mark last shifted entry as empty
		let offt = self.get_offset(index);
		write_all(&self.map, offt, &[0; ENTRY_SIZE as usize]).await?;

		Ok(())
	}

	/// Find an entry with the given name.
	pub async fn find_index(&self, name: &Name) -> Result<Option<RawEntry>, Error<D>> {
		trace!("find_index {:?}", name);
		let hash = self.hasher.hash(name);
		let mut index = hash as u32 & self.mask();
		let dir = Dir::new(self.fs, self.dir_id);

		match self.hasher {
			Hasher::SipHasher13(_) => loop {
				let entry = self.get(index).await?;
				let Some(key) = entry.key.as_ref() else {
					// We encountered an empty entry,
					// which means the entry we seek does not exist.
					return Ok(None);
				};
				if dir.compare_names(key, name, hash).await? {
					// We found the entry we were looking for.
					break Ok(Some(entry));
				}

				// TODO terminate early if PSL differs

				index += 1;
				index &= self.mask();
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
		let offt = self.get_offset(index);

		// Read entry.
		let mut buf = [0; ENTRY_SIZE as usize];
		read_exact(&self.map, offt, &mut buf).await?;

		// Parse entry.
		let [key @ .., a, b, c, d] = buf;
		let key = Key::from_raw(&key);
		let item_index = u32::from_le_bytes([a, b, c, d]);

		Ok(RawEntry { key, index, item_index })
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
		// Get data necessary to write entry.
		let offt = self.get_offset(entry.index);

		// Serialize entry.
		let mut buf = [0; ENTRY_SIZE as usize];
		match entry.key.as_ref() {
			Some(key) => {
				buf[..28].copy_from_slice(&key.to_raw());
				buf[28..].copy_from_slice(&entry.item_index.to_le_bytes());
			}
			None => debug_assert_eq!(entry.item_index, 0),
		}

		// Write out entry.
		write_all(&self.map, offt, &buf).await
	}

	/// Insert a new entry.
	///
	/// Optionally, a new name can be specified.
	/// The `key` present in `entry` will be ignored in that case.
	///
	/// Returns a [`Key`] on success.
	pub async fn insert(
		&self,
		mut entry: RawEntry,
		name: Option<&Name>,
	) -> Result<Option<Key>, Error<D>> {
		trace!("insert {:?} {:?}", (entry.index, &entry.key), &name);

		// Determine hash to use.
		let hash = if let Some(name) = name {
			// Update hash if a name was specified.
			self.hasher.hash(name)
		} else {
			// Take or calculate the hash from the existing key.
			entry.hash(&self.hasher).expect("entry has no name")
		};
		let name = name.map(|name| (name, hash));

		// Update index
		entry.index = hash as u32 & self.mask();

		/// Set the key & insert the entry.
		async fn insert_entry<'a, D: Dev>(
			slf: &HashMap<'a, D>,
			name: Option<(&Name, u64)>,
			mut entry: RawEntry,
		) -> Result<Key, Error<D>> {
			trace!("insert::insert_entry {:?} {:?}", name, &entry);
			let dir = Dir::new(slf.fs, slf.dir_id);

			// Store name if necessary.
			if let Some((name, hash)) = name {
				entry.key = Some(if name.len_u8() <= 27 {
					// Embed key
					let mut data = [0; 27];
					data[..name.len()].copy_from_slice(name.as_ref());
					Key::Embed { len: name.len_nonzero_u8(), data }
				} else {
					// Store key on heap
					let offset = dir.alloc_heap(name.len_u8().into()).await?;
					dir.write_heap(offset, name).await?;
					Key::Heap { len: name.len_nonzero_u8(), offset, hash }
				});
			}

			// Store entry data at index.
			slf.set(&entry)
				.await
				.map(|()| entry.key.expect("expected key on non-empty entry"))
		}

		let dir = Dir::new(self.fs, self.dir_id);

		// Start with default PSL ("poorness")
		let mut entry_psl = 0u32;
		let calc_psl = |i: u32, h: u64| i.wrapping_sub(h as u32) & self.mask();

		match self.hasher {
			Hasher::SipHasher13(_) => {
				// Insert with robin-hood hashing,
				// - First, find an appropriate slot for the entry to be inserted.
				// - Then, shift every other entry forward until we hit an empty slot.

				// Insert current entry.
				let (mut entry, key) = loop {
					let e = self.get(entry.index).await?;

					let Some(key) = e.key.as_ref() else {
						// If None, we found a free slot.
						// Insert and return.
						let key = insert_entry(self, name, entry).await?;
						return Ok(Some(key));
					};

					// If the entry has the same name as us, exit.
					if let Some((name, hash)) = name {
						if dir.compare_names(&key, name, hash).await? {
							return Ok(None);
						}
					}

					// Check if the PSL (Probe Sequence Length) is lower than that of ours
					// If yes, swap with it and begin shifting forward.
					if entry_psl > calc_psl(entry.index, key.hash(&self.hasher)) {
						let key = insert_entry(self, name, entry).await?;
						break (e, key);
					}

					// Try next slot
					entry.index += 1;
					entry.index &= self.mask();
					entry_psl += 1;
				};

				// Shift all elements forward by one slot
				// until an empty slot is found.
				loop {
					entry.index += 1;
					entry.index &= self.mask();

					let e = self.get(entry.index).await?;

					// Insert unconditionally.
					insert_entry(self, None, entry).await?;

					// We found a free slot.
					// Free at last!
					if e.key.is_none() {
						return Ok(Some(key));
					}

					// Continue with swapped out entry.
					entry = e;
				}
			}
		}
	}

	/// Write a full, minimized heap allocation log.
	pub(super) async fn save_heap_alloc_log(&self, log: &RangeSet<u64>) -> Result<(), Error<D>> {
		// Get log.
		let mut log_offt = self.heap_alloc_log_base();

		// Ensure there is enough capacity.
		self.map
			.resize(log_offt + 16 * u64::try_from(log.len()).unwrap())
			.await?;

		// Write log.
		for r in log.iter() {
			let mut buf = [0; 16];
			buf[..8].copy_from_slice(&r.start.to_le_bytes());
			buf[8..].copy_from_slice(&(r.end - r.start).to_le_bytes());
			write_all(&self.map, log_offt, &buf).await?;
			log_offt += 16;
		}

		Ok(())
	}

	/// Get or load the heap allocation map.
	pub(super) async fn heap_alloc_log(&self) -> Result<RefMut<'a, RangeSet<u64>>, Error<D>> {
		// Check if the map has already been loaded.
		let data = self.fs.dir_data(self.dir_id);
		if let Ok(r) = RefMut::filter_map(data, |data| data.heap_alloc_map.as_mut()) {
			return Ok(r);
		}

		// Load the allocation log
		let mut log = RangeSet::new();
		let alloc_log_base = self.heap_alloc_log_base();
		let l = self.map.len().await?;

		for offt in (alloc_log_base..l).step_by(16) {
			let mut buf = [0; 16];
			read_exact(&self.map, offt, &mut buf).await?;
			let [a, b, c, d, e, f, g, h, buf @ ..] = buf;
			let offset = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
			let length = u64::from_le_bytes(buf);
			assert!(length > 0, "todo: return error when length == 0");
			if length > 0 {
				if log.contains(&offset) {
					// Deallocation
					log.remove(offset..offset + length);
				} else {
					// Allocation
					log.insert(offset..offset + length);
				}
			}
		}

		Ok(RefMut::map(self.fs.dir_data(self.dir_id), |data| {
			data.heap_alloc_map.insert(log)
		}))
	}

	/// Determine the offset of an entry.
	///
	/// This does *not* check if the index is in range.
	fn get_offset(&self, index: u32) -> u64 {
		u64::from(index) * u64::from(ENTRY_SIZE)
	}

	/// Offset of the base of the heap allocation log.
	fn heap_alloc_log_base(&self) -> u64 {
		(1u64 << self.size) * u64::from(ENTRY_SIZE)
	}

	/// Mask to apply to indices.
	fn mask(&self) -> u32 {
		1u32.wrapping_shl(self.size.to_raw().into()).wrapping_sub(1)
	}
}

/// Raw entry data.
#[derive(Clone, Debug)]
pub(super) struct RawEntry {
	/// Key data.
	///
	/// The variant depends on whether it is embedded or not.
	/// If the length is `<= 14` the key is embedded.
	/// If the length is `> 14` the key is stored on the heap.
	///
	/// If the key length is 0, this entry is [`None`].
	/// In that case the entry is empty.
	pub key: Option<Key>,
	/// The index of the entry.
	pub index: u32,
	/// The index of the corresponding item.
	pub item_index: u32,
}

impl RawEntry {
	/// Take or calculate the hash from the existing key.
	///
	/// Returns [`None`] if the key is [`None`]
	fn hash(&self, hasher: &Hasher) -> Option<u64> {
		self.key.as_ref().map(|key| key.hash(hasher))
	}
}
