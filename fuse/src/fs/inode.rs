use {
	nrfs::{
		dev::FileDev,
		dir::{ext, ItemRef},
		DirRef, FileRef, Nrfs, RawDirRef, RawFileRef, RawRef, RawSymRef, SymRef, TmpRef,
	},
	std::collections::BTreeMap,
};

const INO_TY_MASK: u64 = 3 << 62;
const INO_TY_DIR: u64 = 0 << 62;
const INO_TY_FILE: u64 = 1 << 62;
const INO_TY_SYM: u64 = 2 << 62;

type Ino = u64;

#[derive(Debug)]
pub struct InodeStore {
	/// Inode to directory map
	dir: BTreeMap<Ino, InodeData<RawDirRef>>,
	/// Inode to file map
	file: BTreeMap<Ino, InodeData<RawFileRef>>,
	/// Inode to sym map
	sym: BTreeMap<Ino, InodeData<RawSymRef>>,
	/// Reverse lookup from directory ID
	dir_rev: BTreeMap<RawDirRef, Ino>,
	/// Reverse lookup from directory ID + file
	file_rev: BTreeMap<RawFileRef, Ino>,
	/// Reverse lookup from directory ID + sym
	sym_rev: BTreeMap<RawSymRef, Ino>,
	/// Inode counter.
	/// Used to generate new inodes.
	ino_counter: u64,
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

macro_rules! impl_ty {
	($ty:ident $val:ident $val_rev:ident $ino:ident | $add:ident $get:ident) => {
		pub fn $add<'a>(
			&mut self,
			$val: $ty<'a, FileDev>,
			incr: bool,
		) -> (u64, Option<$ty<'a, FileDev>>) {
			let (ino, e) = Self::add(
				&mut self.ino_counter,
				&mut self.$val,
				&mut self.$val_rev,
				$val,
				incr,
			);
			(ino | $ino, e)
		}

		pub fn $get<'s, 'a>(
			&'s self,
			fs: &'a Nrfs<FileDev>,
			ino: u64,
		) -> TmpRef<'s, $ty<'a, FileDev>> {
			self.$val[&(ino ^ $ino)].value.into_tmp(fs)
		}
	};
}

impl InodeStore {
	/// Create a new inode store with the given permissions, uid and gid as defaults.
	pub fn new(permissions: u16, uid: u32, gid: u32) -> Self {
		Self {
			unix_default: ext::unix::Entry::new(permissions, uid, gid),
			// Start from one since FUSE uses 1 for the root dir.
			ino_counter: 1,
			dir: Default::default(),
			file: Default::default(),
			sym: Default::default(),
			dir_rev: Default::default(),
			file_rev: Default::default(),
			sym_rev: Default::default(),
		}
	}

	fn add<'a, T: RawRef<'a, FileDev>>(
		counter: &mut u64,
		m: &mut BTreeMap<Ino, InodeData<T::Raw>>,
		rev_m: &mut BTreeMap<T::Raw, Ino>,
		t: T,
		incr: bool,
	) -> (u64, Option<T>)
	where
		T::Raw: Ord + Eq,
	{
		let (h, t) = if let Some(h) = rev_m.get_mut(&t.as_raw()) {
			m.get_mut(h).expect("no item with handle").reference_count += u64::from(incr);
			(*h, Some(t))
		} else {
			let ino = *counter;
			*counter += 1;
			m.insert(ino, InodeData { value: t.as_raw(), reference_count: 1 });
			rev_m.insert(t.into_raw(), ino);
			(ino, None)
		};
		(h, t)
	}

	pub fn get<'s, 'a>(
		&'s self,
		fs: &'a Nrfs<FileDev>,
		ino: u64,
	) -> TmpRef<'s, ItemRef<'a, FileDev>> {
		let h = &(ino & !INO_TY_MASK);
		match ino & INO_TY_MASK {
			INO_TY_DIR => self.dir[h].value.into_tmp(fs).into(),
			INO_TY_FILE => self.file[h].value.into_tmp(fs).into(),
			INO_TY_SYM => self.sym[h].value.into_tmp(fs).into(),
			_ => unreachable!(),
		}
	}

	impl_ty!(DirRef  dir  dir_rev  INO_TY_DIR  | add_dir  get_dir );
	impl_ty!(FileRef file file_rev INO_TY_FILE | add_file get_file);
	impl_ty!(SymRef  sym  sym_rev  INO_TY_SYM  | add_sym  get_sym );

	/// Forget an entry.
	///
	/// Returns an [`ItemRef`] if it needs to be dropped.
	pub fn forget<'a>(
		&mut self,
		fs: &'a Nrfs<FileDev>,
		ino: u64,
		nlookup: u64,
	) -> Option<ItemRef<'a, FileDev>> {
		let h = &(ino & !INO_TY_MASK);
		match ino & INO_TY_MASK {
			INO_TY_DIR => {
				let c = &mut self
					.dir
					.get_mut(h)
					.expect("no dir with handle")
					.reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					let d = self.dir.remove(h).unwrap();
					self.dir_rev.remove(&d.value);
					return Some(DirRef::from_raw(fs, d.value).into());
				}
			}
			INO_TY_FILE => {
				let c = &mut self
					.file
					.get_mut(h)
					.expect("no file with handle")
					.reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					let f = self.file.remove(h).unwrap();
					self.file_rev.remove(&f.value);
					return Some(FileRef::from_raw(fs, f.value).into());
				}
			}
			INO_TY_SYM => {
				let c = &mut self
					.sym
					.get_mut(h)
					.expect("no symlink with handle")
					.reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					let f = self.sym.remove(h).unwrap();
					self.sym_rev.remove(&f.value);
					return Some(SymRef::from_raw(fs, f.value).into());
				}
			}
			_ => unreachable!(),
		}
		None
	}

	/// Drop all references and inodes.
	pub async fn remove_all(&mut self, fs: &Nrfs<FileDev>) {
		for (_, r) in self.dir.drain_filter(|_, _| true) {
			DirRef::from_raw(fs, r.value).drop().await.unwrap();
		}
		for (_, r) in self.file.drain_filter(|_, _| true) {
			FileRef::from_raw(fs, r.value).drop().await.unwrap();
		}
		for (_, r) in self.sym.drain_filter(|_, _| true) {
			SymRef::from_raw(fs, r.value).drop().await.unwrap();
		}

		self.dir_rev.clear();
		self.file_rev.clear();
		self.sym_rev.clear();
	}
}
