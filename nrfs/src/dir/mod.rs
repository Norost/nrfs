pub mod ext;

mod child;
mod dir_data;
mod hasher;
mod hashmap;
mod heap;
mod item;
mod key;

pub use item::{ItemData, ItemRef};

pub(crate) use {child::Child, dir_data::DirData, hasher::Hasher, item::Type};

use {
	crate::{
		file, read_exact, write_all, Background, DataHeader, Dev, DirRef, Error, FileRef, Name,
		Nrfs, SymRef,
	},
	core::{cell::RefMut, mem},
	hashmap::*,
	item::Item,
	key::Key,
	rangemap::RangeSet,
	std::collections::hash_map,
};

// TODO determine a good load factor.
const MAX_LOAD_FACTOR_MILLI: u64 = 875;
const MIN_LOAD_FACTOR_MILLI: u64 = 375;

const MAP_OFFT: u64 = 1;
const HEAP_OFFT: u64 = 2;

/// Constants used to manipulate the directory header.
mod header {
	pub mod offset {
		pub const HASHMAP_SIZE: u16 = 3;
		pub const ITEM_COUNT: u16 = 4;
		pub const ITEM_CAPACITY: u16 = 8;
	}
}

/// Helper structure for working with directories.
#[derive(Debug)]
pub(crate) struct Dir<'a, 'b, D: Dev> {
	/// The filesystem containing the directory's data.
	pub(crate) fs: &'a Nrfs<D>,
	/// The ID of this directory.
	pub(crate) id: u64,
	/// Background task runner.
	pub(crate) bg: &'b Background<'a, D>,
}

impl<'a, 'b, D: Dev> Dir<'a, 'b, D> {
	/// Create a [`Dir`] helper structure.
	pub(crate) fn new(bg: &'b Background<'a, D>, fs: &'a Nrfs<D>, id: u64) -> Self {
		Self { fs, id, bg }
	}

	/// Create a helper structure to operate on the hashmap of this directory.
	async fn hashmap(&self) -> Result<HashMap<'a, 'b, D>, Error<D>> {
		let obj = self.fs.storage.get(self.bg, self.id + MAP_OFFT).await?;
		let data = self.fs.dir_data(self.id);
		Ok(HashMap::new(self, &data, obj, data.hashmap_size))
	}

	/// Set the type of an entry.
	pub(crate) async fn set_ty(&self, index: u32, ty: Type) -> Result<(), Error<D>> {
		trace!("set_ty {:?} {:?}", index, ty);
		self.set(index, 28, &ty.to_raw()).await
	}

	/// Compare an entry's key with the given name.
	///
	/// `hash` is used to avoid redundant heap reads.
	async fn compare_names(&self, key: &Key, name: &Name, hash: u64) -> Result<bool, Error<D>> {
		trace!(
			"compare_names {:?} ({:?}, {:#10x})",
			key,
			<&Name>::try_from(name).unwrap(),
			hash
		);
		match key {
			&Key::Embed { data, len } => Ok(&data[..len.get().into()] == &**name),
			&Key::Heap { offset, len, hash: e_hash } => {
				if e_hash != hash || usize::from(len.get()) != name.len() {
					return Ok(false);
				}
				let mut buf = vec![0; len.get().into()];
				let obj = self.fs.storage.get(self.bg, self.id + HEAP_OFFT).await?;
				crate::read_exact(&obj, offset, &mut buf).await?;
				Ok(&*buf == &**name)
			}
		}
	}

	/// Remove a specific entry.
	///
	/// Returns `true` if successful.
	/// It will fail for entries whose type is unknown to avoid space leaks.
	async fn remove_at(&self, entry: &RawEntry) -> Result<Result<(), RemoveError>, Error<D>> {
		trace!("remove_at {:?}", entry);

		let item = self.get(entry.item_index).await?;

		// Only destroy types we recognize
		match item.ty {
			Type::None => todo!(),
			Type::Unknown(_) => return Ok(Err(RemoveError::UnknownType)),
			_ => {}
		}

		// If the entry is a directory, first check if it is empty or not.
		if let Type::Dir { id } = item.ty {
			// Ensure the directory is empty to avoid space leaks.
			let buf = &mut [0; 4];
			let dir = self.fs.storage.get(self.bg, id).await?;
			read_exact(&dir, header::offset::ITEM_COUNT.into(), buf).await?;
			let item_count = u32::from_le_bytes(*buf);
			if item_count > 0 {
				return Ok(Err(RemoveError::NotEmpty));
			}
		}

		// Remove from map.
		self.update_item_count(|x| x - 1).await?;
		self.hashmap().await?.remove_at(entry.index).await?;

		// Deallocate key if stored on heap
		match entry.key {
			None | Some(Key::Embed { .. }) => {}
			Some(Key::Heap { offset, len, .. }) => {
				self.dealloc_heap(offset, len.get().into()).await?
			}
		}

		let data = self.fs.dir_data(self.id);
		let item_len = data.item_size();
		let has_live_ref = data.children.contains_key(&item.index);

		if has_live_ref {
			// If a child is present, don't remove the item yet as we don't want dangling
			// references.

			// If loaded, mark as dangling.
			match data.children.get(&item.index) {
				None => drop(data),
				Some(&Child::File(idx)) => {
					drop(data);
					debug_assert!(!self.fs.file_data(idx).is_dangling, "already dangling");
					self.fs.file_data(idx).is_dangling = true;
				}
				Some(&Child::Dir(id)) => {
					drop(data);
					debug_assert!(!self.fs.dir_data(id).is_dangling, "already dangling");
					self.fs.dir_data(id).is_dangling = true;
				}
			}
			// Clear the key to mark as dangling.
			self.set(item.index, 0, &[0; 28]).await?;
		} else {
			// Destroy the item.
			drop(data);
			match item.ty {
				Type::None | Type::Unknown(_) => todo!(),
				Type::File { id } | Type::Sym { id } => {
					// Dereference object.
					self.fs
						.storage
						.get(self.bg, id)
						.await?
						.decrease_reference_count()
						.await?;
				}
				Type::Dir { id } => {
					// Dereference dir, map and heap.
					for i in 0..3 {
						self.fs
							.storage
							.get(self.bg, id + i)
							.await?
							.decrease_reference_count()
							.await?;
					}
				}
				Type::EmbedFile { offset, length } | Type::EmbedSym { offset, length } => {
					// Free heap space.
					self.dealloc_heap(offset, length.into()).await?;
				}
			}
			self.dealloc_item_slot(item.index).await?;
			// Clear the item entirely.
			self.set(item.index, 0, &vec![0; item_len.into()]).await?;
		}

		// Check if we should shrink the hashmap
		if self.fs.dir_data(self.id).should_shrink() {
			self.shrink().await?;
		}
		Ok(Ok(()))
	}

	/// Rename an entry.
	///
	/// Returns `false` if the entry could not be found or another entry with the same index
	/// exists.
	async fn rename(&self, from: &Name, to: &Name) -> Result<Result<(), RenameError>, Error<D>> {
		trace!("rename {:?} -> {:?}", from, to);
		let map = self.hashmap().await?;
		let Some(entry) = map.find_index(from).await?
			else { return Ok(Err(RenameError::NotFound)) };

		// Remove entry.
		map.remove_at(entry.index).await?;

		// Try to insert entry with new name.
		let old_entry = entry.clone();
		let item_index = entry.item_index;
		if let Some(key) = map.insert(entry, Some(to)).await? {
			// Update key in item.
			self.set(item_index, 0, &key.to_raw()).await?;
			Ok(Ok(()))
		} else {
			// On failure, restore entry.
			let r = map.insert(old_entry, None).await?;
			debug_assert!(r.is_some(), "failed to insert after remove");
			Ok(Err(RenameError::Duplicate))
		}
	}

	/// Update the entry count.
	async fn update_item_count(&self, f: impl FnOnce(u32) -> u32) -> Result<(), Error<D>> {
		trace!("update_item_count");
		let mut data = self.fs.dir_data(self.id);
		let count = f(data.item_count);
		data.item_count = count;
		drop(data);
		let obj = self.fs.storage.get(self.bg, self.id).await?;
		crate::write_all(&obj, 4, &count.to_le_bytes()).await
	}

	/// Move an entry to another directory.
	async fn transfer(
		&self,
		name: &Name,
		to_dir: u64,
		to_name: &Name,
	) -> Result<Result<(), TransferError>, Error<D>> {
		trace!("transfer {:?} {:?} {:?}", name, to_dir, to_name);

		// 1. Find the entry + item to transfer.
		// 2. (if embedded) allocate on heap in other dir & update item.
		// 3. Try to insert entry + item in other dir.
		// 4. (if embedded) copy to other dir & deallocate in current.
		// 5. Remove entry + item in this dir.
		// 6. Transfer child, if present.

		if self.id == to_dir {
			// Don't transfer, rename instead.
			return Ok(match self.rename(name, to_name).await? {
				Ok(()) => Ok(()),
				Err(RenameError::NotFound) => Err(TransferError::NotFound),
				Err(RenameError::Duplicate) => Err(TransferError::Duplicate),
			});
		}

		let to_dir = Dir::new(self.bg, self.fs, to_dir);

		let from_map = self.hashmap().await?;

		// 1. Find the entry + item to transfer.
		let Some(entry) = from_map.find_index(name).await?
			else { return Ok(Err(TransferError::NotFound)) };
		let mut item = self.get(entry.item_index).await?;
		debug_assert!(item.data.key.is_some(), "item to transfer is not in use");

		// If we don't know the type, don't transfer to avoid bringing the filesystem in an
		// inconsistent state.
		match item.ty {
			Type::None => todo!("none type (corrupt fs?)"),
			Type::Unknown(_) => return Ok(Err(TransferError::UnknownType)),
			_ => {}
		}

		// If the entry is a directory, ensure it is not a ancestor of to_dir
		if let Type::Dir { id } = item.ty {
			// Start from to_dir and work downwards to the root.
			// The root is guaranteed to be the ancestor of all other objects.
			let mut cur_id = to_dir.id;
			while cur_id != 0 {
				if cur_id == id {
					// to_dir is a descendant of the entry to be moved, so cancel operation.
					return Ok(Err(TransferError::IsAncestor));
				}
				cur_id = self.fs.dir_data(cur_id).header.parent_id;
			}
		}

		// 2. (if embedded) allocate on heap in other dir & update item.
		let from_embed_data = match &mut item.ty {
			Type::EmbedFile { offset, length } | Type::EmbedSym { offset, length } => {
				let from_offset = *offset;
				let to_offset = to_dir.alloc_heap((*length).into()).await?;
				*offset = to_offset;
				Some((from_offset, to_offset, *length))
			}
			Type::Dir { .. } | Type::File { .. } | Type::Sym { .. } => None,
			Type::Unknown(_) | Type::None => unreachable!(),
		};

		// 3. Try to insert entry + item in other dir.
		let ext = Extensions { unix: item.data.ext_unix, mtime: item.data.ext_mtime };
		let to_index = match to_dir.insert(to_name, item.ty, &ext).await? {
			Ok(i) => i,
			Err(InsertError::Full) => return Ok(Err(TransferError::Full)),
			Err(InsertError::Duplicate) => return Ok(Err(TransferError::Duplicate)),
			Err(InsertError::Dangling) => return Ok(Err(TransferError::Dangling)),
		};

		// 4. (if embedded) copy to other dir & deallocate in current.
		if let Some((from_offset, to_offset, length)) = from_embed_data {
			let buf = &mut vec![0; length.into()];
			self.read_heap(from_offset, buf).await?;
			self.dealloc_heap(from_offset, length.into()).await?;
			to_dir.write_heap(to_offset, buf).await?;
		}

		// 5. Remove entry + item in this dir.
		from_map.remove_at(entry.index).await?;
		self.dealloc_item_slot(item.index).await?;
		let item_len = self.fs.dir_data(self.id).item_size();
		self.set(item.index, 0, &vec![0; item_len.into()]).await?;
		// Deallocate key if stored on heap
		match entry.key {
			None | Some(Key::Embed { .. }) => {}
			Some(Key::Heap { offset, len, .. }) => {
				self.dealloc_heap(offset, len.get().into()).await?
			}
		}
		self.update_item_count(|x| x - 1).await?;

		// 6. Transfer child, if present.
		let mut data = self.fs.dir_data(self.id);
		if let Some(child) = data.children.remove(&item.index) {
			// Dereference current dir
			data.header.reference_count -= 1;
			drop(data);

			// Move to other dir and increase refcount
			let mut data = self.fs.dir_data(to_dir.id);
			data.children.insert(to_index, child);
			data.header.reference_count += 1;
			drop(data);

			// Fixup child
			let mut header = match child {
				Child::File(idx) => {
					let mut data = self.fs.file_data(idx);
					if let Some((_, offset, length)) = from_embed_data {
						// Fixup pointer to embedded data.
						debug_assert!(matches!(&data.inner, file::Inner::Embed { .. }));
						data.inner = file::Inner::Embed { offset, length };
					} else {
						debug_assert!(matches!(&data.inner, file::Inner::Object { .. }));
					}
					RefMut::map(data, |d| &mut d.header)
				}
				Child::Dir(id) => {
					debug_assert!(from_embed_data.is_none(), "dir is never embedded");
					RefMut::map(self.fs.dir_data(id), |d| &mut d.header)
				}
			};
			header.parent_id = to_dir.id;
			header.parent_index = to_index;
		}

		Ok(Ok(()))
	}

	/// Resize the hashmap
	///
	/// `grow` indicates whether the size of the map should increase or decrease.
	async fn resize(&self, grow: bool) -> Result<(), Error<D>> {
		trace!("resize {}", if grow { "grow" } else { "shrink" });
		// Since we're going to load the entire log we can as well minimize it.
		self.heap_alloc_log().await?;

		let data = self.fs.dir_data(self.id);

		let hashmap_size = data.hashmap_size;
		let capacity = 1u64 << hashmap_size;
		let item_count = data.item_count;

		let new_size = if grow {
			// FIXME don't panic, we should just fail.
			DirSize::from_raw(hashmap_size.to_raw() + 1).unwrap()
		} else {
			debug_assert!(
				u64::from(item_count) < capacity / 2,
				"not enough free slots"
			);
			DirSize::from_raw(hashmap_size.to_raw() - 1).expect("hashmap is already at min size")
		};

		drop(data);

		// Create hashmap helpers
		let cur_map = self.hashmap().await?;
		let new_map = HashMap::create(self, new_size).await?;

		// Copy entries
		for index in (0..capacity).map(|i| i as _) {
			let entry = cur_map.get(index).await?;
			if entry.key.is_none() {
				continue;
			}
			let r = new_map.insert(entry, None).await?;
			debug_assert!(r.is_some(), "failed to insert entry in new map");
		}

		// Replace old map
		self.fs
			.storage
			.get(self.bg, self.id + MAP_OFFT)
			.await?
			.replace_with(new_map.map)
			.await?;
		let mut data = self.fs.dir_data(self.id);
		data.hashmap_size = new_size;
		drop(data);
		let obj = self.fs.storage.get(self.bg, self.id).await?;
		crate::write_all(
			&obj,
			header::offset::HASHMAP_SIZE.into(),
			&[new_size.to_raw()],
		)
		.await?;
		self.save_heap_alloc_log().await
	}

	/// Grow the hashmap
	async fn grow(&self) -> Result<(), Error<D>> {
		self.resize(true).await
	}

	/// Shrink the hashmap.
	///
	/// There must be *at least* `capacity / 2 + 1` slots free,
	/// i.e. `item_count < capacity / 2`.
	async fn shrink(&self) -> Result<(), Error<D>> {
		self.resize(false).await
	}

	/// Try to insert a new item.
	async fn insert(
		&self,
		name: &Name,
		ty: Type,
		ext: &Extensions,
	) -> Result<Result<u32, InsertError>, Error<D>> {
		trace!("insert {:?} {:?} {:?}", name, &ty, ext);
		let data = self.fs.dir_data(self.id);
		if data.is_dangling {
			// If dangling, refuse to insert new entries as the dir should stay empty.
			return Ok(Err(InsertError::Dangling));
		}
		let item_len = data.item_size();
		let should_grow = data.should_grow();
		drop(data);

		// Check if we should grow the hashmap
		if should_grow {
			self.grow().await?;
		}

		// Allocate an item slot.
		let Some(item_index) = self.alloc_item_slot().await?
			else { return Ok(Err(InsertError::Full)) };

		let entry = RawEntry { key: None, index: u32::MAX, item_index };

		if let Some(key) = self.hashmap().await?.insert(entry, Some(name)).await? {
			// Write out entry.
			let item = Item {
				ty,
				data: ItemData { key: Some(key), ext_unix: ext.unix, ext_mtime: ext.mtime },
				index: u32::MAX, // Doesn't matter, not used.
			};
			let mut buf = vec![0; item_len.into()];
			item.to_raw(&self.fs.dir_data(self.id), &mut buf);
			self.set(item_index, 0, &buf).await?;
			self.update_item_count(|x| x + 1).await?;
			Ok(Ok(item_index))
		} else {
			// Deallocate item slot and give up.
			self.dealloc_item_slot(item_index).await?;
			Ok(Err(InsertError::Duplicate))
		}
	}

	/// Get an item.
	async fn get(&self, index: u32) -> Result<Item, Error<D>> {
		trace!("get {:?}", index);
		let d = self.fs.dir_data(self.id);
		let offt = u64::from(d.header_len()) + u64::from(d.item_size()) * u64::from(index);
		let item_len = d.item_size();
		drop(d);
		let mut buf = vec![0; item_len.into()];
		let obj = self.fs.storage.get(self.bg, self.id).await?;
		crate::read_exact(&obj, offt, &mut buf).await?;
		let item = Item::from_raw(&self.fs.dir_data(self.id), &buf, index);
		Ok(item)
	}

	/// Set any item data at an arbitrary offset.
	async fn set(&self, index: u32, offset: u16, data: &[u8]) -> Result<(), Error<D>> {
		trace!("set {:?} {:?}:{:?}", index, offset, data.len());
		let d = self.fs.dir_data(self.id);
		let offt = u64::from(d.header_len())
			+ u64::from(d.item_size()) * u64::from(index)
			+ u64::from(offset);
		drop(d);
		let obj = self.fs.storage.get(self.bg, self.id).await?;
		crate::write_all(&obj, offt, data).await?;
		Ok(())
	}

	/// Allocate an item slot.
	async fn alloc_item_slot(&self) -> Result<Option<u32>, Error<D>> {
		trace!("alloc_item_slot");
		let mut log = self.item_alloc_log().await?;

		// Take first free slot.
		let Some(index) = log.gaps(&(0..u32::MAX)).next().map(|r| r.start)
			else { return Ok(None) };
		log.insert(index..index + 1);
		drop(log);

		// If the slot is over the item capacity, resize.
		// Incidentally, it also there are no gaps.
		let mut data = self.fs.dir_data(self.id);
		if data.item_capacity <= index {
			data.item_capacity = index + 1;
			drop(data);
			let obj = self.fs.storage.get(self.bg, self.id).await?;
			crate::write_all(
				&obj,
				header::offset::ITEM_CAPACITY.into(),
				&(index + 1).to_le_bytes(),
			)
			.await?;
		} else {
			drop(data);
		}

		// Save log
		self.save_item_alloc_log().await?;
		Ok(Some(index))
	}

	/// Deallocate an item slot.
	async fn dealloc_item_slot(&self, index: u32) -> Result<(), Error<D>> {
		trace!("dealloc_item_slot {:?}", index);
		let mut log = self.item_alloc_log().await?;
		debug_assert!(log.contains(&index), "double free");
		log.remove(index..index + 1);
		drop(log);
		self.save_item_alloc_log().await?;
		Ok(())
	}

	/// Write a full, minimized item allocation log.
	async fn save_item_alloc_log(&self) -> Result<(), Error<D>> {
		trace!("save_item_alloc_log");
		let log = self.item_alloc_log().await?.clone();
		let data = self.fs.dir_data(self.id);
		let base = u64::from(data.header_len())
			+ u64::from(data.item_size()) * u64::from(data.item_capacity);
		drop(data);

		// Write log
		let obj = self.fs.storage.get(self.bg, self.id).await?;
		obj.resize(base + u64::try_from(log.len()).unwrap() * 8)
			.await?;
		for (i, r) in log.iter().enumerate() {
			let offt = u64::try_from(i).unwrap() * 8;
			let mut buf = [0; 8];
			buf[..4].copy_from_slice(&r.start.to_le_bytes());
			buf[4..].copy_from_slice(&(r.end - r.start).to_le_bytes());
			write_all(&obj, base + offt, &mut buf).await?;
		}

		Ok(())
	}

	/// Get or load the item allocation map.
	async fn item_alloc_log(&self) -> Result<RefMut<'a, RangeSet<u32>>, Error<D>> {
		trace!("item_alloc_log");
		let data = self.fs.dir_data(self.id);
		let data = match RefMut::filter_map(data, |d| d.item_alloc_map.as_mut()) {
			Ok(log) => return Ok(log),
			Err(data) => data,
		};
		let base = u64::from(data.header_len())
			+ u64::from(data.item_size()) * u64::from(data.item_capacity);
		drop(data);

		// Read log
		let mut log = RangeSet::new();
		let obj = self.fs.storage.get(self.bg, self.id).await?;
		let len = obj.len().await?;
		for offt in (base..len).step_by(8) {
			let mut buf = [0; 8];
			read_exact(&obj, offt, &mut buf).await?;
			let [a, b, c, d, length @ ..] = buf;
			let offset = u32::from_le_bytes([a, b, c, d]);
			let length = u32::from_le_bytes(length);
			assert!(length > 0, "todo: return error if length == 0");
			if log.contains(&offset) {
				// Deallocation
				log.remove(offset..offset + length);
			} else {
				// Allocation
				log.insert(offset..offset + length);
			}
		}

		// Insert log
		Ok(RefMut::map(self.fs.dir_data(self.id), |d| {
			d.item_alloc_map.insert(log)
		}))
	}

	/// Clear the given item.
	/// This is used to clean up removed items after all references are destroyed.
	///
	/// # Note
	///
	/// This function does *not* free up space!
	pub(crate) async fn clear_item(&self, index: u32) -> Result<(), Error<D>> {
		trace!("clear_item {:?}", index);
		let len = self.fs.dir_data(self.id).item_size() - 28;
		self.set(index, 28, &vec![0; len.into()]).await
	}
}

impl<'a, 'b, D: Dev> DirRef<'a, 'b, D> {
	/// Create a new directory.
	pub(crate) async fn new(
		parent_dir: &Dir<'a, 'b, D>,
		parent_index: u32,
		options: &DirOptions,
	) -> Result<DirRef<'a, 'b, D>, Error<D>> {
		// Increase reference count to parent directory.
		let mut parent = parent_dir.fs.dir_data(parent_dir.id);
		parent.header.reference_count += 1;
		debug_assert!(
			!parent.children.contains_key(&parent_index),
			"child present in parent"
		);

		// Load directory data.
		drop(parent);
		let dir_ref = Self::new_inner(
			parent_dir.bg,
			parent_dir.fs,
			parent_dir.id,
			parent_index,
			options,
		)
		.await?;

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
		bg: &'b Background<'a, D>,
		fs: &'a Nrfs<D>,
		options: &DirOptions,
	) -> Result<DirRef<'a, 'b, D>, Error<D>> {
		Self::new_inner(bg, fs, u64::MAX, u32::MAX, options).await
	}

	/// Create a new directory.
	///
	/// This does not directly create a reference to a parent directory.
	async fn new_inner(
		bg: &'b Background<'a, D>,
		fs: &'a Nrfs<D>,
		parent_id: u64,
		parent_index: u32,
		options: &DirOptions,
	) -> Result<DirRef<'a, 'b, D>, Error<D>> {
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
		let slf_id = fs.storage.create_many(bg, 3).await?;

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
		let obj = fs.storage.get(bg, slf_id).await?;
		crate::write_grow(&obj, 0, &buf[..header_len]).await?;
		drop(obj);

		// Ensure hashmap is properly sized.
		fs.storage
			.get(bg, slf_id + MAP_OFFT)
			.await?
			.resize(32)
			.await?;

		// Insert directory data & return reference.
		fs.data.borrow_mut().directories.insert(slf_id, data);
		Ok(Self { fs, bg, id: slf_id })
	}

	/// Load an existing directory.
	pub(crate) async fn load(
		parent_dir: &Dir<'a, 'b, D>,
		parent_index: u32,
		id: u64,
	) -> Result<DirRef<'a, 'b, D>, Error<D>> {
		trace!("load {:?} | {:?}:{:?}", id, parent_dir.id, parent_index);
		// Check if the directory is already present in the filesystem object.
		//
		// If so, just reference that and return.
		if let Some(dir) = parent_dir.fs.data.borrow_mut().directories.get_mut(&id) {
			dir.header.reference_count += 1;
			return Ok(DirRef { fs: parent_dir.fs, bg: parent_dir.bg, id });
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
		let dir_ref = Self::load_inner(
			parent_dir.bg,
			parent_dir.fs,
			parent_dir.id,
			parent_index,
			id,
		)
		.await?;

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
	pub(crate) async fn load_root(
		bg: &'b Background<'a, D>,
		fs: &'a Nrfs<D>,
	) -> Result<DirRef<'a, 'b, D>, Error<D>> {
		trace!("load_root");
		// Check if the root directory is already present in the filesystem object.
		//
		// If so, just reference that and return.
		if let Some(dir) = fs.data.borrow_mut().directories.get_mut(&0) {
			dir.header.reference_count += 1;
			return Ok(DirRef { fs, bg, id: 0 });
		}

		// FIXME check if the directory is already being loaded
		// Also create some guard when we start fetching directory data.

		// Load directory data.
		let dir_ref = Self::load_inner(bg, fs, u64::MAX, u32::MAX, 0).await?;

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
		bg: &'b Background<'a, D>,
		fs: &'a Nrfs<D>,
		parent_id: u64,
		parent_index: u32,
		id: u64,
	) -> Result<DirRef<'a, 'b, D>, Error<D>> {
		trace!("load_inner {:?} | {:?}:{:?}", id, parent_id, parent_index);
		let obj = fs.storage.get(bg, id).await?;

		// Get basic info
		let mut buf = [0; 32];
		crate::read_exact(&obj, 0, &mut buf).await?;
		let [header_len8, item_len8, hash_algorithm, hashmap_size_p2, rem @ ..] = buf;
		let [a, b, c, d, rem @ ..] = rem;
		let item_count = u32::from_le_bytes([a, b, c, d]);
		let [a, b, c, d, rem @ ..] = rem;
		let item_capacity = u32::from_le_bytes([a, b, c, d]);
		let [_, _, _, _, hash_key @ ..] = rem;

		// FIXME return error
		let hashmap_size = DirSize::from_raw(hashmap_size_p2).unwrap();

		// Get extensions
		let mut unix_offset = None;
		let mut mtime_offset = None;
		let mut offt = 32;
		// An extension consists of at least two bytes, ergo +1
		while offt + 1 < u16::from(header_len8) * 8 {
			let mut buf = [0; 2];
			crate::read_exact(&obj, offt.into(), &mut buf).await?;

			let [name_len, data_len] = buf;
			let total_len = u16::from(name_len) + u16::from(data_len);

			let mut buf = [0; 255 * 2];
			crate::read_exact(&obj, u64::from(offt) + 2, &mut buf[..total_len.into()]).await?;

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
		Ok(Self { fs, bg, id })
	}

	/// Get a list of enabled extensions.
	///
	/// This only includes extensions that are understood by this library.
	pub fn enabled_extensions(&self) -> EnableExtensions {
		let data = self.fs.dir_data(self.id);
		let mut ext = EnableExtensions::default();
		data.mtime_offset.is_some().then(|| ext.add_mtime());
		data.unix_offset.is_some().then(|| ext.add_unix());
		ext
	}

	/// Create a new file.
	///
	/// This fails if an entry with the given name already exists.
	pub async fn create_file(
		&self,
		name: &Name,
		ext: &Extensions,
	) -> Result<Result<FileRef<'a, 'b, D>, InsertError>, Error<D>> {
		trace!("create_file {:?}", name);
		let ty = Type::EmbedFile { offset: 0, length: 0 };
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
	) -> Result<Result<DirRef<'a, 'b, D>, InsertError>, Error<D>> {
		trace!("create_dir {:?}", name);
		// Try to insert stub entry
		let ty = Type::Dir { id: u64::MAX };
		let index = match self.dir().insert(name, ty, ext).await? {
			Ok(i) => i,
			Err(e) => return Ok(Err(e)),
		};
		// Create new directory with stub index (u32::MAX).
		let d = DirRef::new(&self.dir(), index, options).await?;
		// Fixup ID in entry.
		self.dir().set_ty(index, Type::Dir { id: d.id }).await?;
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
	) -> Result<Result<SymRef<'a, 'b, D>, InsertError>, Error<D>> {
		trace!("create_sym {:?}", name);
		let ty = Type::EmbedSym { offset: 0, length: 0 };
		let index = self.dir().insert(name, ty, ext).await?;
		Ok(index.map(|i| SymRef::from_embed(&self.dir(), 0, 0, i)))
	}

	/// Retrieve the entry with an index equal or greater than `index`.
	///
	/// Used for iteration.
	pub async fn next_from(
		&self,
		mut index: u64,
	) -> Result<Option<(ItemRef<'a, 'b, D>, u64)>, Error<D>> {
		trace!("next_from {:?}", index);
		while index < u64::from(self.fs.dir_data(self.id).item_capacity) {
			// Get standard info
			let item = self.dir().get(index.try_into().unwrap()).await?;

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
	pub async fn find(&self, name: &Name) -> Result<Option<ItemRef<'a, 'b, D>>, Error<D>> {
		trace!("find {:?}", name);
		let dir = self.dir();
		if let Some(entry) = dir.hashmap().await?.find_index(name).await? {
			let item = self.dir().get(entry.item_index).await?;
			Ok(Some(ItemRef::new(&dir, &item).await?))
		} else {
			Ok(None)
		}
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
		to_dir: &DirRef<'a, 'b, D>,
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
		if let Some(e) = self.dir().hashmap().await?.find_index(name).await? {
			self.dir().remove_at(&e).await
		} else {
			Ok(Err(RemoveError::NotFound))
		}
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
			let DirRef { id, bg, fs } = self;
			mem::forget(self);

			let mut fs_ref = fs.data.borrow_mut();
			let hash_map::Entry::Occupied(mut data) = fs_ref.directories.entry(id) else {
				unreachable!()
			};
			data.get_mut().header.reference_count -= 1;
			if data.get().header.reference_count == 0 {
				// Remove DirData.
				let header = data.get().header.clone();
				let data = data.remove();

				// If this is the root dir there is no parent dir,
				// so check first.
				if id != 0 {
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
					self = DirRef { fs, bg, id: header.parent_id };

					// If this directory is dangling, destroy it
					// and remove it from the parent directory.
					let r = async {
						if data.is_dangling && !fs.read_only {
							// TODO add sanity checks to ensure it is empty.
							for offt in 0..3 {
								fs.storage
									.get(bg, id + offt)
									.await?
									.decrease_reference_count()
									.await?;
							}
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

	/// Get the amount of items in this directory.
	pub async fn len(&self) -> Result<u32, Error<D>> {
		Ok(self.fs.dir_data(self.id).item_count)
	}

	/// Create a [`Dir`] helper structure.
	pub(crate) fn dir(&self) -> Dir<'a, 'b, D> {
		Dir::new(self.bg, self.fs, self.id)
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
