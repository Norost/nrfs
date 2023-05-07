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
	pub fn add(&mut self, key: Key) -> u64 {
		*self
			.rev_map
			.entry(*key.key())
			.and_modify(|h| {
				self.map
					.get_mut(h)
					.expect("no item with handle")
					.reference_count += 1;
			})
			.or_insert_with(|| {
				let ino = self.ino_counter;
				self.ino_counter += 1;
				self.map.insert(ino, InodeData { key, reference_count: 1 });
				ino
			})
	}

	pub fn get(&self, ino: u64) -> Option<Get> {
		self.map
			.get(&ino)
			.map(|d| Get::Key(d.key))
			.or_else(|| self.stale.contains_key(&ino).then_some(Get::Stale))
	}

	pub fn set(&mut self, ino: u64, key: ItemKey) {
		let r = self.map.get_mut(&ino).expect("no item with ino");
		self.rev_map.remove(r.key.key()).unwrap();
		let prev = self.rev_map.insert(key, ino);
		assert!(prev.is_none(), "key with multiple ino");
		*r.key.key_mut() = key;
	}

	pub fn get_ino(&self, key: ItemKey) -> Option<u64> {
		self.rev_map.get(&key).copied()
	}

	pub fn mark_stale(&mut self, ino: u64) {
		let data = self.map.remove(&ino).expect("no item with ino");
		self.rev_map
			.remove(data.key.key())
			.expect("no item with key");
		self.stale.insert(ino, data.reference_count);
	}

	/// Forget an entry.
	pub fn forget(&mut self, ino: u64, nlookup: u64) {
		if let Some(d) = self.map.get_mut(&ino) {
			d.reference_count -= nlookup;
			(d.reference_count == 0).then(|| self.map.remove(&ino));
		} else if let Some(refc) = self.stale.get_mut(&ino) {
			*refc -= nlookup;
			(*refc == 0).then(|| self.stale.remove(&ino));
		}
	}
}

pub enum Get {
	Key(Key),
	Stale,
}
