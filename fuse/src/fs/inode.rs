use std::collections::btree_map::Entry;

use {nrfs::ItemKey, std::collections::BTreeMap};

#[derive(Clone, Copy, Debug)]
pub enum Key {
	Dir(ItemKey),
	File(ItemKey),
	Sym(ItemKey),
}

impl Key {
	pub fn key(&self) -> &ItemKey {
		match self {
			Self::Dir(k) | Self::File(k) | Self::Sym(k) => k,
		}
	}

	pub fn key_mut(&mut self) -> &mut ItemKey {
		match self {
			Self::Dir(k) | Self::File(k) | Self::Sym(k) => k,
		}
	}
}

#[derive(Debug)]
pub struct InodeStore {
	map: BTreeMap<u64, InodeData>,
	rev_map: BTreeMap<ItemKey, u64>,
	stale: BTreeMap<u64, u64>,
	/// Used to generate new inodes.
	ino_counter: u64,
}

#[derive(Debug)]
struct InodeData {
	key: Key,
	reference_count: u64,
	parent_ino: u64,
	generation: u64,
}

impl InodeStore {
	pub fn new() -> Self {
		Self {
			// Start from one since FUSE uses 1 for the root dir.
			ino_counter: 1,
			map: Default::default(),
			rev_map: Default::default(),
			stale: Default::default(),
		}
	}

	/// Add an entry.
	///
	/// If the entry was already present,
	/// the reference count is increased.
	pub fn add(&mut self, key: Key, parent_ino: u64, generation: u64) -> u64 {
		match self.rev_map.entry(*key.key()) {
			Entry::Occupied(e) => {
				self.map
					.get_mut(e.get())
					.expect("no item with handle")
					.reference_count += 1;
				*e.get()
			}
			Entry::Vacant(e) => {
				let ino = self.ino_counter;
				self.ino_counter += 1;
				self.map.insert(
					ino,
					InodeData { key, reference_count: 1, parent_ino, generation },
				);
				e.insert(ino);
				self.map
					.get_mut(&parent_ino)
					.expect("no parent")
					.reference_count += 1;
				ino
			}
		}
	}

	pub fn get(&mut self, ino: u64) -> Option<Get<'_>> {
		self.map
			.get_mut(&ino)
			.map(|d| Get::Key(d.key, d.parent_ino, &mut d.generation))
			.or_else(|| self.stale.contains_key(&ino).then_some(Get::Stale))
	}

	pub fn set(&mut self, ino: u64, key: ItemKey, parent_ino: u64) {
		let r = self.map.get_mut(&ino).expect("no item with ino");
		self.rev_map.remove(r.key.key()).unwrap();
		let prev = self.rev_map.insert(key, ino);
		assert!(prev.is_none(), "key with multiple ino");
		*r.key.key_mut() = key;
		r.parent_ino = parent_ino;
	}

	pub fn get_ino(&self, key: ItemKey) -> Option<u64> {
		self.rev_map.get(&key).copied()
	}

	pub fn mark_stale(&mut self, ino: u64) {
		let data = self.map.remove(&ino).expect("no item with ino");
		self.map
			.get_mut(&data.parent_ino)
			.expect("no parent")
			.reference_count -= 1;
		self.rev_map
			.remove(data.key.key())
			.expect("no item with key");
		self.stale.insert(ino, data.reference_count);
	}

	/// Forget an entry.
	pub fn forget(&mut self, ino: u64, nlookup: u64) {
		if let Some(d) = self.map.get_mut(&ino) {
			d.reference_count -= nlookup;
			if d.reference_count == 0 {
				let data = self.map.remove(&ino).expect("no data");
				self.rev_map.remove(data.key.key());
				self.forget(data.parent_ino, 1);
			}
		} else if let Some(refc) = self.stale.get_mut(&ino) {
			*refc -= nlookup;
			if *refc == 0 {
				self.stale.remove(&ino);
			}
		} else {
			panic!("invalid ino {}", ino);
		}
	}
}

pub enum Get<'a> {
	Key(Key, u64, &'a mut u64),
	Stale,
}
