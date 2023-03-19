use {
	nrfs::{DirKey, FileKey, ItemKey, Unix},
	std::collections::{btree_map::Entry, BTreeMap, BTreeSet},
};

const INO_TY_MASK: u64 = 3 << 62;
const INO_TY_DIR: u64 = 0 << 62;
const INO_TY_FILE: u64 = 1 << 62;
const INO_TY_SYM: u64 = 2 << 62;

type Ino = u64;

#[derive(Debug)]
pub struct InodeStore {
	/// Inode to directory map
	dir: BTreeMap<Ino, InodeData<DirKey>>,
	/// Inode to file map
	file: BTreeMap<Ino, InodeData<FileKey>>,
	/// Inode to sym map
	sym: BTreeMap<Ino, InodeData<FileKey>>,
	/// Reverse lookup from directory ID
	dir_rev: BTreeMap<DirKey, Ino>,
	/// Reverse lookup from directory ID + file
	file_rev: BTreeMap<FileKey, Ino>,
	/// Reverse lookup from directory ID + sym
	sym_rev: BTreeMap<FileKey, Ino>,
	unlinked: BTreeSet<Ino>,
	/// Inode counter.
	/// Used to generate new inodes.
	ino_counter: u64,
	/// Default key for `unix` extension if not present.
	pub unix_default: Unix,
}

/// "inode" data
#[derive(Debug)]
struct InodeData<K> {
	/// What this "inode" actually points to.
	key: K,
	/// The amount of references to this inode.
	reference_count: u64,
}

macro_rules! impl_ty {
	($ty:ident $val:ident $val_rev:ident $ino:ident | $add:ident $get:ident) => {
		pub fn $add(&mut self, $val: $ty) -> Ino {
			$ino | Self::add(
				&mut self.ino_counter,
				&mut self.$val,
				&mut self.$val_rev,
				$val,
			)
		}

		pub fn $get(&self, ino: u64) -> $ty {
			self.$val[&(ino ^ $ino)].key
		}
	};
}

impl InodeStore {
	/// Create a new inode store with the given permissions, uid and gid as defaults.
	pub fn new(permissions: u16, uid: u32, gid: u32) -> Self {
		Self {
			unix_default: Unix::new(permissions, uid, gid),
			// Start from one since FUSE uses 1 for the root dir.
			ino_counter: 1,
			dir: Default::default(),
			file: Default::default(),
			sym: Default::default(),
			dir_rev: Default::default(),
			file_rev: Default::default(),
			sym_rev: Default::default(),
			unlinked: Default::default(),
		}
	}

	/// Add an entry.
	///
	/// If the entry was already present,
	/// the reference count is increased.
	fn add<K: Copy + Ord + Eq>(
		counter: &mut u64,
		m: &mut BTreeMap<Ino, InodeData<K>>,
		rev_m: &mut BTreeMap<K, Ino>,
		key: K,
	) -> u64 {
		if let Some(h) = rev_m.get_mut(&key) {
			m.get_mut(h).expect("no item with handle").reference_count += 1;
			*h
		} else {
			let ino = *counter;
			*counter += 1;
			m.insert(ino, InodeData { key, reference_count: 1 });
			rev_m.insert(key, ino);
			ino
		}
	}

	pub fn get(&self, ino: u64) -> ItemKey {
		let h = &(ino & !INO_TY_MASK);
		match ino & INO_TY_MASK {
			INO_TY_DIR => ItemKey::Dir(self.dir[h].key),
			INO_TY_FILE => ItemKey::File(self.file[h].key),
			INO_TY_SYM => ItemKey::Sym(self.sym[h].key),
			_ => unreachable!(),
		}
	}

	pub fn set(&mut self, ino: u64, key: ItemKey) {
		let h = ino & !INO_TY_MASK;
		match ino & INO_TY_MASK {
			INO_TY_DIR => {
				let key = key.into_dir().unwrap();
				let Entry::Occupied(e) = self.dir.entry(h) else { todo!() };
				self.dir_rev.remove(&e.get().key);
				e.into_mut().key = key;
				self.dir_rev.insert(key, h);
			}
			INO_TY_FILE => {
				let key = key.into_file().unwrap();
				let Entry::Occupied(e) = self.file.entry(h) else { todo!() };
				self.file_rev.remove(&e.get().key);
				e.into_mut().key = key;
				self.file_rev.insert(key, h);
			}
			INO_TY_SYM => {
				let key = key.into_sym().unwrap();
				let Entry::Occupied(e) = self.sym.entry(h) else { todo!() };
				self.sym_rev.remove(&e.get().key);
				e.into_mut().key = key;
				self.sym_rev.insert(key, h);
			}
			_ => unreachable!(),
		}
	}

	impl_ty!(DirKey  dir  dir_rev  INO_TY_DIR  | add_dir  get_dir );
	impl_ty!(FileKey file file_rev INO_TY_FILE | add_file get_file);
	impl_ty!(FileKey sym  sym_rev  INO_TY_SYM  | add_sym  get_sym );

	pub fn get_ino(&self, key: ItemKey) -> Option<Ino> {
		match key {
			ItemKey::Dir(k) => self.dir_rev.get(&k).map(|&i| i | INO_TY_DIR),
			ItemKey::File(k) => self.file_rev.get(&k).map(|&i| i | INO_TY_FILE),
			ItemKey::Sym(k) => self.sym_rev.get(&k).map(|&i| i | INO_TY_SYM),
		}
	}

	pub fn is_unlinked(&self, ino: Ino) -> bool {
		self.unlinked.contains(&ino)
	}

	pub fn mark_unlinked(&mut self, ino: Ino) {
		self.unlinked.insert(ino);
	}

	/// Forget an entry.
	///
	/// If it was the last reference, return the key and whether it was unlinked.
	pub fn forget(&mut self, ino: u64, nlookup: u64) -> Option<(ItemKey, bool)> {
		let h = &(ino & !INO_TY_MASK);
		let ret = match ino & INO_TY_MASK {
			INO_TY_DIR => {
				let c = &mut self
					.dir
					.get_mut(h)
					.expect("no dir with handle")
					.reference_count;
				*c = c.saturating_sub(nlookup);
				(*c == 0).then(|| {
					let d = self.dir.remove(h).unwrap();
					self.dir_rev.remove(&d.key);
					ItemKey::Dir(d.key)
				})
			}
			INO_TY_FILE => {
				let c = &mut self
					.file
					.get_mut(h)
					.expect("no file with handle")
					.reference_count;
				*c = c.saturating_sub(nlookup);
				(*c == 0).then(|| {
					let f = self.file.remove(h).unwrap();
					self.file_rev.remove(&f.key);
					ItemKey::File(f.key)
				})
			}
			INO_TY_SYM => {
				let c = &mut self
					.sym
					.get_mut(h)
					.expect("no symlink with handle")
					.reference_count;
				*c = c.saturating_sub(nlookup);
				(*c == 0).then(|| {
					let f = self.sym.remove(h).unwrap();
					self.sym_rev.remove(&f.key);
					ItemKey::Sym(f.key)
				})
			}
			_ => unreachable!(),
		};
		ret.map(|r| (r, self.unlinked.remove(&ino)))
	}
}
