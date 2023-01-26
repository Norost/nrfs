use {
	arena::{Arena, Handle},
	core::hash::Hash,
	nrfs::{
		dev::FileDev,
		dir::{ext, ItemRef},
		Background, DirRef, FileRef, Nrfs, RawDirRef, RawFileRef, RawRef, RawSymRef, SymRef,
		TmpRef,
	},
	std::collections::HashMap,
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

macro_rules! impl_ty {
	($ty:ident $val:ident $val_rev:ident $ino:ident | $add:ident $get:ident) => {
		pub fn $add<'a, 'b>(
			&mut self,
			$val: $ty<'a, 'b, FileDev>,
			incr: bool,
		) -> (u64, Option<$ty<'a, 'b, FileDev>>) {
			let (ino, e) = Self::add(&mut self.$val, &mut self.$val_rev, $val, incr);
			(ino | $ino, e)
		}

		pub fn $get<'s, 'a, 'b>(
			&'s self,
			fs: &'a Nrfs<FileDev>,
			bg: &'b Background<'a, FileDev>,
			ino: u64,
		) -> TmpRef<'s, $ty<'a, 'b, FileDev>> {
			self.$val[Handle::from_raw((ino ^ $ino) as usize - 1, ())]
				.value
				.into_tmp(fs, bg)
		}
	};
}

impl InodeStore {
	/// Create a new inode store with the given uid and gid as defaults.
	pub fn new(uid: u32, gid: u32) -> Self {
		Self { unix_default: ext::unix::Entry::new(0o700, uid, gid), ..Default::default() }
	}

	fn add<'a, 'b, T: RawRef<'a, 'b, FileDev>>(
		m: &mut Arena<InodeData<T::Raw>, ()>,
		rev_m: &mut HashMap<T::Raw, Handle<()>>,
		t: T,
		incr: bool,
	) -> (u64, Option<T>)
	where
		T::Raw: Hash + Eq,
	{
		let (h, t) = if let Some(h) = rev_m.get_mut(&t.as_raw()) {
			m[*h].reference_count += u64::from(incr);
			(*h, Some(t))
		} else {
			let h = m.insert(InodeData { value: t.as_raw(), reference_count: 1 });
			rev_m.insert(t.into_raw(), h);
			(h, None)
		};
		// Because ROOT_ID (1) is reserved for the root dir, but nrfs uses 0 for the root dir
		(h.into_raw().0 as u64 + 1, t)
	}

	pub fn get<'s, 'a, 'b>(
		&'s self,
		fs: &'a Nrfs<FileDev>,
		bg: &'b Background<'a, FileDev>,
		ino: u64,
	) -> TmpRef<'s, ItemRef<'a, 'b, FileDev>> {
		let h = Handle::from_raw((ino & !INO_TY_MASK) as usize - 1, ());
		match ino & INO_TY_MASK {
			INO_TY_DIR => self.dir[h].value.into_tmp(fs, bg).into(),
			INO_TY_FILE => self.file[h].value.into_tmp(fs, bg).into(),
			INO_TY_SYM => self.sym[h].value.into_tmp(fs, bg).into(),
			_ => unreachable!(),
		}
	}

	impl_ty!(DirRef  dir  dir_rev  INO_TY_DIR  | add_dir  get_dir );
	impl_ty!(FileRef file file_rev INO_TY_FILE | add_file get_file);
	impl_ty!(SymRef  sym  sym_rev  INO_TY_SYM  | add_sym  get_sym );

	/// Forget an entry.
	///
	/// Returns an [`ItemRef`] if it needs to be dropped.
	pub fn forget<'a, 'b>(
		&mut self,
		fs: &'a Nrfs<FileDev>,
		bg: &'b Background<'a, FileDev>,
		ino: u64,
		nlookup: u64,
	) -> Option<ItemRef<'a, 'b, FileDev>> {
		let h = Handle::from_raw((ino & !INO_TY_MASK) as usize - 1, ());
		match ino & INO_TY_MASK {
			INO_TY_DIR => {
				let c = &mut self.dir[h].reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					let d = self.dir.remove(h).unwrap();
					self.dir_rev.remove(&d.value);
					return Some(DirRef::from_raw(fs, bg, d.value).into());
				}
			}
			INO_TY_FILE => {
				let c = &mut self.file[h].reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					let f = self.file.remove(h).unwrap();
					self.file_rev.remove(&f.value);
					return Some(FileRef::from_raw(fs, bg, f.value).into());
				}
			}
			INO_TY_SYM => {
				let c = &mut self.sym[h].reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					let f = self.sym.remove(h).unwrap();
					self.sym_rev.remove(&f.value);
					return Some(SymRef::from_raw(fs, bg, f.value).into());
				}
			}
			_ => unreachable!(),
		}
		None
	}

	/// Drop all references and inodes.
	pub async fn remove_all<'a, 'b>(
		&mut self,
		fs: &'a Nrfs<FileDev>,
		bg: &'b Background<'a, FileDev>,
	) {
		for (_, r) in self.dir.drain() {
			DirRef::from_raw(fs, bg, r.value).drop().await.unwrap();
		}
		for (_, r) in self.file.drain() {
			FileRef::from_raw(fs, bg, r.value).drop().await.unwrap();
		}
		for (_, r) in self.sym.drain() {
			SymRef::from_raw(fs, bg, r.value).drop().await.unwrap();
		}

		self.dir_rev.clear();
		self.file_rev.clear();
		self.sym_rev.clear();
	}
}
