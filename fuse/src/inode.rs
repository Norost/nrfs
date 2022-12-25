use {
	arena::{Arena, Handle},
	core::hash::Hash,
	nrfs::{
		dev::FileDev,
		dir::{ext, ItemRef},
		DirRef, FileRef, Nrfs, RawDirRef, RawFileRef, RawRef, RawSymRef, SymRef, TmpRef,
	},
	std::collections::{HashMap, HashSet},
};

const INO_TY_MASK: u64 = 3 << 62;
const INO_TY_DIR: u64 = 0 << 62;
const INO_TY_FILE: u64 = 1 << 62;
const INO_TY_SYM: u64 = 2 << 62;

#[derive(Debug, Default)]
pub struct InodeStore {
	/// Inode to directory map
	dir: Arena<InodeData<RawDirRef>, ()>,
	/// Inode to file map
	file: Arena<InodeData<RawFileRef>, ()>,
	/// Inode to sym map
	sym: Arena<InodeData<RawSymRef>, ()>,
	/// Reverse lookup from directory ID
	dir_rev: HashMap<RawDirRef, Handle<()>>,
	/// Reverse lookup from directory ID + file
	file_rev: HashMap<RawFileRef, Handle<()>>,
	/// Reverse lookup from directory ID + sym
	sym_rev: HashMap<RawSymRef, Handle<()>>,
	/// Default value for `unix` extension if not present.
	pub unix_default: nrfs::dir::ext::unix::Entry,
}

/// "inode" data
#[derive(Debug)]
struct InodeData<T> {
	/// What this "inode" actually points to.
	value: T,
	/// The amount of references to this inode.
	reference_count: u64,
}

impl InodeStore {
	/// Create a new inode store with the given uid and gid as defaults.
	pub fn new(uid: u32, gid: u32) -> Self {
		Self { unix_default: ext::unix::Entry::new(0o700, uid, gid), ..Default::default() }
	}

	pub fn add_dir<'f>(&mut self, dir: DirRef<'f, FileDev>, incr: bool) -> u64 {
		Self::add(&mut self.dir, &mut self.dir_rev, dir, incr) | INO_TY_DIR
	}

	pub fn add_file<'f>(&mut self, file: FileRef<'f, FileDev>, incr: bool) -> u64 {
		Self::add(&mut self.file, &mut self.file_rev, file, incr) | INO_TY_FILE
	}

	pub fn add_sym<'f>(&mut self, sym: SymRef<'f, FileDev>, incr: bool) -> u64 {
		Self::add(&mut self.sym, &mut self.sym_rev, sym, incr) | INO_TY_SYM
	}

	fn add<'f, T: RawRef<'f, FileDev>>(
		m: &mut Arena<InodeData<T::Raw>, ()>,
		rev_m: &mut HashMap<T::Raw, Handle<()>>,
		t: T,
		incr: bool,
	) -> u64
	where
		T::Raw: Hash + Eq,
	{
		let h = if let Some(h) = rev_m.get_mut(&t.as_raw()) {
			m[*h].reference_count += u64::from(incr);
			*h
		} else {
			let h = m.insert(InodeData { value: t.as_raw(), reference_count: 1 });
			rev_m.insert(t.into_raw(), h);
			h
		};
		// Because ROOT_ID (1) is reserved for the root dir, but nrfs uses 0 for the root dir
		h.into_raw().0 as u64 + 1
	}

	pub fn get<'s, 'f>(
		&'s self,
		fs: &'f Nrfs<FileDev>,
		ino: u64,
	) -> TmpRef<'s, ItemRef<'f, FileDev>> {
		let h = Handle::from_raw((ino & !INO_TY_MASK) as usize - 1, ());
		match ino & INO_TY_MASK {
			INO_TY_DIR => self.dir[h].value.into_tmp(fs).into(),
			INO_TY_FILE => self.file[h].value.into_tmp(fs).into(),
			INO_TY_SYM => self.sym[h].value.into_tmp(fs).into(),
			_ => unreachable!(),
		}
	}

	pub fn get_dir<'s, 'f>(
		&'s self,
		fs: &'f Nrfs<FileDev>,
		ino: u64,
	) -> TmpRef<'s, DirRef<'f, FileDev>> {
		self.dir[Handle::from_raw((ino ^ INO_TY_DIR) as usize - 1, ())]
			.value
			.into_tmp(fs)
	}

	pub fn get_file<'s, 'f>(
		&'s self,
		fs: &'f Nrfs<FileDev>,
		ino: u64,
	) -> TmpRef<'s, FileRef<'f, FileDev>> {
		self.file[Handle::from_raw((ino ^ INO_TY_FILE) as usize - 1, ())]
			.value
			.into_tmp(fs)
	}

	pub fn get_sym<'s, 'f>(
		&'s self,
		fs: &'f Nrfs<FileDev>,
		ino: u64,
	) -> TmpRef<'s, SymRef<'f, FileDev>> {
		self.sym[Handle::from_raw((ino ^ INO_TY_SYM) as usize - 1, ())]
			.value
			.into_tmp(fs)
	}

	/// Forget an entry.
	pub fn forget<'f>(&mut self, fs: &'f Nrfs<FileDev>, ino: u64, nlookup: u64) {
		let h = Handle::from_raw((ino & !INO_TY_MASK) as usize - 1, ());
		match ino & INO_TY_MASK {
			INO_TY_DIR => {
				let c = &mut self.dir[h].reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					let d = self.dir.remove(h).unwrap();
					self.dir_rev.remove(&d.value);
					DirRef::from_raw(fs, d.value);
				}
			}
			INO_TY_FILE => {
				let c = &mut self.file[h].reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					let f = self.file.remove(h).unwrap();
					self.file_rev.remove(&f.value);
					FileRef::from_raw(fs, f.value);
				}
			}
			INO_TY_SYM => {
				let c = &mut self.sym[h].reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					let f = self.sym.remove(h).unwrap();
					self.sym_rev.remove(&f.value);
					SymRef::from_raw(fs, f.value);
				}
			}
			_ => unreachable!(),
		}
	}

	/// Drop all references and inodes.
	pub fn remove_all(&mut self, fs: &Nrfs<FileDev>) {
		self.dir.drain().for_each(|(_, r)| {
			DirRef::from_raw(fs, r.value);
		});
		self.file.drain().for_each(|(_, r)| {
			FileRef::from_raw(fs, r.value);
		});
		self.sym.drain().for_each(|(_, r)| {
			SymRef::from_raw(fs, r.value);
		});

		self.dir_rev.clear();
		self.file_rev.clear();
		self.sym_rev.clear();
	}
}
