use {
	arena::{Arena, Handle},
	fuser::*,
	log::{debug, trace},
	nrfs::{
		dev::{FileDev, FileDevError},
		dir::{Entry, EntryData},
		DirOptions, DirRef, FileRef, Name, Nrfs, RawDirRef, RawFileRef, RawSymRef, SymRef, TmpRef,
	},
	std::{
		collections::HashMap,
		ffi::OsStr,
		fs,
		hash::Hash,
		io::{self, Read, Seek, SeekFrom, Write},
		os::unix::ffi::OsStrExt,
		path::Path,
		rc::Rc,
		time::{Duration, SystemTime, UNIX_EPOCH},
	},
};

const TTL: Duration = Duration::MAX;

const INO_TY_MASK: u64 = 3 << 62;
const INO_TY_DIR: u64 = 0 << 62;
const INO_TY_FILE: u64 = 1 << 62;
const INO_TY_SYM: u64 = 2 << 62;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	env_logger::init();

	let mut a = std::env::args().skip(1);
	let f = a.next().ok_or("expected file path")?;
	let m = a.next().ok_or("expected mount path")?;

	let f = fs::OpenOptions::new().read(true).write(true).open(&f)?;
	let f = futures_executor::block_on(Fs::new(f));
	fuser::mount2(
		f,
		m,
		&[
			MountOption::FSName("nrfs".into()),
			MountOption::DefaultPermissions,
		],
	)?;
	Ok(())
}

struct Fs {
	fs: Nrfs<FileDev>,
	ino: InodeStore,
}

/// "inode" data
struct InodeData<T> {
	/// What this "inode" actually points to.
	value: T,
	/// The amount of references to this inode.
	reference_count: u64,
}

#[derive(Default)]
struct InodeStore {
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
	unix_default: nrfs::dir::ext::unix::Entry,
}

impl Fs {
	async fn new(io: fs::File) -> Self {
		let global_cache_size = 1 << 24;
		let dirty_cache_size = 1 << 23;

		let fs = FileDev::new(io, nrfs::BlockSize::K4);
		let fs = Nrfs::load([fs].into(), global_cache_size, dirty_cache_size)
			.await
			.unwrap();
		let unix_default =
			nrfs::dir::ext::unix::Entry::new(0o700, unsafe { libc::getuid() }, unsafe {
				libc::getgid()
			});
		let mut s = Self { fs, ino: InodeStore { unix_default, ..Default::default() } };

		// Add root dir now so it's always at ino 1.
		let root = s.fs.root_dir().await.unwrap();
		let ino = s.ino.add_dir(root.into_raw(), true);
		dbg!(ino);
		s
	}

	/// Convert [`EntryData`] et al. to [`FileAttr`].
	fn attr(&self, ino: u64, ty: FileType, len: u64, data: &EntryData) -> FileAttr {
		let u = data.ext_unix.unwrap_or(self.ino.unix_default);

		let mtime = data.ext_mtime.map_or(UNIX_EPOCH, |t| {
			if t.mtime > 0 {
				UNIX_EPOCH.checked_add(Duration::from_millis(t.mtime as _))
			} else {
				UNIX_EPOCH.checked_sub(Duration::from_millis(-i128::from(t.mtime) as _))
			}
			.unwrap()
		});

		let blksize = 1u32 << self.fs.block_size().to_raw();
		FileAttr {
			atime: UNIX_EPOCH,
			mtime,
			ctime: UNIX_EPOCH,
			crtime: UNIX_EPOCH,
			perm: u.permissions,
			nlink: 1,
			uid: u.uid(),
			gid: u.gid(),
			rdev: 0,
			flags: 0,
			kind: ty,
			size: len,
			blocks: ((u128::from(len) + u128::from(blksize) - 1) / u128::from(blksize))
				.try_into()
				.unwrap_or(u64::MAX),
			ino,
			blksize,
		}
	}
}

impl InodeStore {
	fn add_dir(&mut self, dir: RawDirRef, incr: bool) -> u64 {
		Self::add(&mut self.dir, &mut self.dir_rev, dir, incr) | INO_TY_DIR
	}

	fn add_file(&mut self, file: RawFileRef, incr: bool) -> u64 {
		Self::add(&mut self.file, &mut self.file_rev, file, incr) | INO_TY_FILE
	}

	fn add_sym(&mut self, sym: RawSymRef, incr: bool) -> u64 {
		Self::add(&mut self.sym, &mut self.sym_rev, sym, incr) | INO_TY_SYM
	}

	fn add<T: Clone + Hash + Eq>(
		m: &mut Arena<InodeData<T>, ()>,
		rev_m: &mut HashMap<T, Handle<()>>,
		t: T,
		incr: bool,
	) -> u64 {
		let h = if let Some(h) = rev_m.get_mut(&t) {
			m[*h].reference_count += u64::from(incr);
			*h
		} else {
			let h = m.insert(InodeData { value: t.clone(), reference_count: 1 });
			rev_m.insert(t, h);
			h
		};
		// Because ROOT_ID (1) is reserved for the root dir, but nrfs uses 0 for the root dir
		h.into_raw().0 as u64 + 1
	}

	fn get<'s, 'f>(&'s self, fs: &'f Nrfs<FileDev>, ino: u64) -> TmpRef<'s, Entry<'f, FileDev>> {
		let h = Handle::from_raw((ino & !INO_TY_MASK) as usize - 1, ());
		match ino & INO_TY_MASK {
			INO_TY_DIR => self.dir[h].value.into_tmp(fs).into(),
			INO_TY_FILE => self.file[h].value.into_tmp(fs).into(),
			INO_TY_SYM => self.sym[h].value.into_tmp(fs).into(),
			_ => unreachable!(),
		}
	}

	fn get_dir<'s, 'f>(
		&'s self,
		fs: &'f Nrfs<FileDev>,
		ino: u64,
	) -> TmpRef<'s, DirRef<'f, FileDev>> {
		self.dir[Handle::from_raw((ino ^ INO_TY_DIR) as usize - 1, ())]
			.value
			.into_tmp(fs)
	}

	fn get_file<'s, 'f>(
		&'s self,
		fs: &'f Nrfs<FileDev>,
		ino: u64,
	) -> TmpRef<'s, FileRef<'f, FileDev>> {
		self.file[Handle::from_raw((ino ^ INO_TY_FILE) as usize - 1, ())]
			.value
			.into_tmp(fs)
	}

	fn get_sym<'s, 'f>(
		&'s self,
		fs: &'f Nrfs<FileDev>,
		ino: u64,
	) -> TmpRef<'s, SymRef<'f, FileDev>> {
		self.sym[Handle::from_raw((ino ^ INO_TY_SYM) as usize - 1, ())]
			.value
			.into_tmp(fs)
	}

	fn forget(&mut self, fs: &Nrfs<FileDev>, ino: u64, nlookup: u64) {
		let h = Handle::from_raw((ino & !INO_TY_MASK) as usize - 1, ());
		match ino & INO_TY_MASK {
			INO_TY_DIR => {
				let c = &mut self.dir[h].reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					debug!("remove dir {:?}", h);
					let d = self.dir.remove(h).unwrap().value;
					self.dir_rev.remove(&d);
					DirRef::from_raw(fs, d);
				}
			}
			INO_TY_FILE => {
				let c = &mut self.file[h].reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					debug!("remove file {:?}", h);
					let f = self.file.remove(h).unwrap().value;
					self.file_rev.remove(&f);
					FileRef::from_raw(fs, f);
				}
			}
			INO_TY_SYM => {
				let c = &mut self.sym[h].reference_count;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					debug!("remove sym {:?}", h);
					let f = self.sym.remove(h).unwrap().value;
					self.sym_rev.remove(&f);
					SymRef::from_raw(fs, f);
				}
			}
			_ => unreachable!(),
		}
	}

	fn get_unix(&self, entry: &nrfs::dir::EntryData) -> nrfs::dir::ext::unix::Entry {
		entry.ext_unix.unwrap_or(self.unix_default)
	}

	fn get_mtime(&self, entry: &nrfs::dir::EntryData) -> nrfs::dir::ext::mtime::Entry {
		entry.ext_mtime.unwrap_or_default()
	}
}

impl Filesystem for Fs {
	fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), i32> {
		trace!("init");
		use fuser::consts::*;
		const CAP: u32 =
			FUSE_ASYNC_READ | FUSE_BIG_WRITES | FUSE_WRITE_CACHE | FUSE_NO_OPEN_SUPPORT;
		config.add_capabilities(CAP).unwrap();
		Ok(())
	}

	fn lookup(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
		trace!("lookup");
		futures_executor::block_on(async move {
			let d = self.ino.get_dir(&self.fs, parent);

			let Ok(name) = name.as_bytes().try_into() else { return reply.error(libc::ENAMETOOLONG) };
			let Some(entry) = d.find(name).await.unwrap() else { return reply.error(libc::ENOENT) };

			let data = entry.data().await.unwrap();

			// Get type, len, add to inode store
			let (ty, len, ino) = match entry {
				Entry::Dir(d) => {
					let len = d.len().await.unwrap().into();
					let ino = self.ino.add_dir(d.into_raw(), true);
					(FileType::Directory, len, ino)
				}
				Entry::File(f) => {
					let len = f.len().await.unwrap();
					let ino = self.ino.add_file(f.into_raw(), true);
					(FileType::RegularFile, len, ino)
				}
				Entry::Sym(f) => {
					let len = f.len().await.unwrap();
					let ino = self.ino.add_sym(f.into_raw(), true);
					(FileType::Symlink, len, ino)
				}
				Entry::Unknown(_) => todo!("unknown entry type"),
			};

			reply.entry(&TTL, &self.attr(ino, ty, len, &data), 0)
		})
	}

	fn forget(&mut self, _req: &Request<'_>, ino: u64, nlookup: u64) {
		trace!("forget");
		self.ino.forget(&self.fs, ino, nlookup)
	}

	fn getattr(&mut self, _: &Request, ino: u64, reply: ReplyAttr) {
		trace!("getattr");
		futures_executor::block_on(async move {
			let entry = self.ino.get(&self.fs, ino);

			// Get type, len
			let (ty, len) = match &*entry {
				Entry::Dir(d) => (FileType::Directory, d.len().await.unwrap().into()),
				Entry::File(f) => (FileType::RegularFile, f.len().await.unwrap()),
				Entry::Sym(f) => (FileType::Symlink, f.len().await.unwrap()),
				Entry::Unknown(_) => unreachable!(),
			};

			let data = entry.data().await.unwrap();

			reply.attr(&TTL, &self.attr(ino, ty, len, &data));
		})
	}

	fn setattr(
		&mut self,
		_req: &Request<'_>,
		ino: u64,
		mode: Option<u32>,
		uid: Option<u32>,
		gid: Option<u32>,
		size: Option<u64>,
		_atime: Option<TimeOrNow>,
		mtime: Option<TimeOrNow>,
		_ctime: Option<SystemTime>,
		_fh: Option<u64>,
		_crtime: Option<SystemTime>,
		_chgtime: Option<SystemTime>,
		_bkuptime: Option<SystemTime>,
		_flags: Option<u32>,
		reply: ReplyAttr,
	) {
		trace!("setattr");
		futures_executor::block_on(async move {
			// Get entry
			let e = self.ino.get(&self.fs, ino);

			// Set size, if possible
			let (ty, size) = match &*e {
				Entry::Dir(d) => (FileType::Directory, d.len().await.unwrap().into()),
				Entry::File(f) => {
					let len = if let Some(size) = size {
						f.resize(size).await.unwrap();
						size
					} else {
						f.len().await.unwrap()
					};
					(FileType::RegularFile, len)
				}
				Entry::Sym(f) => {
					let len = if let Some(size) = size {
						f.resize(size).await.unwrap();
						size
					} else {
						f.len().await.unwrap()
					};
					(FileType::Symlink, len)
				}
				Entry::Unknown(_) => unreachable!(),
			};

			// Set extension data
			let mut data = e.data().await.unwrap();

			if let Some(ext) = &mut data.ext_unix {
				if mode.is_some() || uid.is_some() || gid.is_some() {
					mode.map(|m| ext.permissions = m as u16 & 0o777);
					uid.map(|u| ext.set_uid(u));
					gid.map(|g| ext.set_gid(g));
					e.set_ext_unix(ext).await.unwrap();
				}
			}

			if let Some(ext) = &mut data.ext_mtime {
				if let Some(mtime) = mtime {
					*ext = match mtime {
						TimeOrNow::Now => mtime_now(),
						TimeOrNow::SpecificTime(t) => mtime_sys(t),
					};
					e.set_ext_mtime(ext).await.unwrap();
				}
			}

			self.fs.finish_transaction().await.unwrap();

			reply.attr(&TTL, &self.attr(ino, ty, size, &data));
		})
	}

	fn read(
		&mut self,
		_req: &Request,
		ino: u64,
		_fh: u64,
		offset: i64,
		size: u32,
		_flags: i32,
		_lock: Option<u64>,
		reply: ReplyData,
	) {
		trace!("read");
		futures_executor::block_on(async move {
			let mut buf = vec![0; size as _];
			let f = self.ino.get_file(&self.fs, ino);
			let l = f.read(offset as _, &mut buf).await.unwrap();
			reply.data(&buf[..l]);
		})
	}

	fn write(
		&mut self,
		_req: &Request,
		ino: u64,
		_fh: u64,
		offset: i64,
		data: &[u8],
		_write_flags: u32,
		_flags: i32,
		_lock_owner: Option<u64>,
		reply: ReplyWrite,
	) {
		trace!("write");
		futures_executor::block_on(async move {
			let f = self.ino.get_file(&self.fs, ino);

			f.write_grow(offset as _, data).await.unwrap();
			reply.written(data.len() as _);

			self.fs.finish_transaction().await.unwrap();
		})
	}

	fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
		trace!("readlink");
		futures_executor::block_on(async move {
			let mut buf = [0; 1 << 15];
			let f = self.ino.get_sym(&self.fs, ino);
			let l = f.read(0, &mut buf).await.unwrap();
			reply.data(&buf[..l]);
		})
	}

	fn readdir(
		&mut self,
		_req: &Request<'_>,
		ino: u64,
		_fh: u64,
		mut offset: i64,
		mut reply: ReplyDirectory,
	) {
		trace!("readdir");
		futures_executor::block_on(async move {
			if offset == 0 {
				if reply.add(ino, 1, FileType::Directory, ".") {
					return reply.ok();
				}
				offset += 1;
			}

			if offset == 1 {
				if reply.add(ino, 2, FileType::Directory, "..") {
					return reply.ok();
				}
				offset += 1;
			}

			let mut d = self.ino.get_dir(&self.fs, ino);

			let mut index = Some(offset as u32 - 2);
			while let Some((e, i)) = async {
				if let Some(i) = index {
					d.next_from(i).await.unwrap()
				} else {
					None
				}
			}
			.await
			{
				let data = e.data().await.unwrap();
				let name = e.key(&data).await.unwrap();

				let (ty, e_ino) = match e {
					Entry::Dir(d) => (FileType::Directory, self.ino.add_dir(d.into_raw(), false)),
					Entry::File(f) => (
						FileType::RegularFile,
						self.ino.add_file(f.into_raw(), false),
					),
					Entry::Sym(f) => (FileType::Symlink, self.ino.add_sym(f.into_raw(), false)),
					Entry::Unknown(_) => todo!("miscellaneous file type"),
				};
				d = self.ino.get_dir(&self.fs, ino);

				let offt = i.map(|i| i64::from(i) + 2).unwrap_or(i64::MAX);
				if reply.add(e_ino, offt, ty, OsStr::from_bytes(&name)) {
					break;
				}
				index = i;
			}

			reply.ok();
		})
	}

	fn create(
		&mut self,
		req: &Request,
		parent: u64,
		name: &OsStr,
		mode: u32,
		_umask: u32,
		_flags: i32,
		reply: ReplyCreate,
	) {
		trace!("create");
		futures_executor::block_on(async move {
			let d = self.ino.get_dir(&self.fs, parent);

			let Ok(name) = name.as_bytes().try_into() else { return reply.error(libc::ENAMETOOLONG) };
			let unix = nrfs::dir::ext::unix::Entry::new(mode as _, req.uid(), req.gid());
			let mtime = mtime_now();
			let ext = nrfs::dir::Extensions {
				unix: Some(unix),
				mtime: Some(mtime),
				..Default::default()
			};
			let Some(f) = d.create_file(name, &ext).await.unwrap() else { return reply.error(libc::EEXIST) };
			let ino = self.ino.add_file(f.into_raw(), false);
			let data = self.ino.get(&self.fs, ino).data().await.unwrap();
			reply.created(
				&TTL,
				&self.attr(ino, FileType::RegularFile, 0, &data),
				0,
				0,
				0,
			);
			self.fs.finish_transaction().await.unwrap();
		})
	}

	fn fallocate(
		&mut self,
		_req: &Request<'_>,
		ino: u64,
		_fh: u64,
		_offset: i64,
		length: i64,
		_mode: i32,
		reply: ReplyEmpty,
	) {
		trace!("fallocate");
		futures_executor::block_on(async move {
			match &*self.ino.get(&self.fs, ino) {
				Entry::Dir(_) => reply.error(libc::EISDIR),
				Entry::File(f) => f.resize(length as _).await.unwrap(),
				Entry::Sym(f) => f.resize(length as _).await.unwrap(),
				Entry::Unknown(_) => unreachable!(),
			}
		})
	}

	fn symlink(
		&mut self,
		req: &Request<'_>,
		parent: u64,
		name: &OsStr,
		link: &Path,
		reply: ReplyEntry,
	) {
		trace!("symlink");
		futures_executor::block_on(async move {
			let d = self.ino.get_dir(&self.fs, parent);
			let Ok(name) = name.as_bytes().try_into() else { return reply.error(libc::ENAMETOOLONG) };
			let unix = nrfs::dir::ext::unix::Entry::new(0o777, req.uid(), req.gid());
			let mtime = mtime_now();
			let ext = nrfs::dir::Extensions {
				unix: Some(unix),
				mtime: Some(mtime),
				..Default::default()
			};
			if let Some(f) = d.create_sym(name, &ext).await.unwrap() {
				let link = link.as_os_str().as_bytes();
				f.write_grow(0, link).await.unwrap();
				let ino = self.ino.add_sym(f.into_raw(), false);
				let data = self.ino.get(&self.fs, ino).data().await.unwrap();
				let attr = self.attr(ino, FileType::Symlink, link.len() as _, &data);
				reply.entry(&TTL, &attr, 0);
			} else {
				reply.error(libc::EEXIST);
			}
		})
	}

	fn mkdir(
		&mut self,
		req: &Request<'_>,
		parent: u64,
		name: &OsStr,
		mode: u32,
		_umask: u32,
		reply: ReplyEntry,
	) {
		trace!("mkdir");
		futures_executor::block_on(async move {
			let d = self.ino.get_dir(&self.fs, parent);

			let Ok(name) = name.as_bytes().try_into() else { return reply.error(libc::ENAMETOOLONG) };

			let unix = nrfs::dir::ext::unix::Entry::new(mode as _, req.uid(), req.gid());
			let mtime = mtime_now();
			let ext = nrfs::dir::Extensions { unix: Some(unix), ..Default::default() };
			let opt = nrfs::DirOptions {
				extensions: *nrfs::dir::EnableExtensions::default()
					.add_unix()
					.add_mtime(),
				..nrfs::dir::DirOptions::new(&[0; 16]) // FIXME randomize
			};
			if let Some(dd) = d.create_dir(name, &opt, &ext).await.unwrap() {
				let ino = self.ino.add_dir(dd.into_raw(), false);
				let data = self.ino.get(&self.fs, ino).data().await.unwrap();
				let attr = self.attr(ino, FileType::Directory, 0, &data);
				reply.entry(&TTL, &attr, 0);
			} else {
				reply.error(libc::EEXIST);
			}
		})
	}

	fn rename(
		&mut self,
		_: &Request<'_>,
		parent: u64,
		name: &OsStr,
		newparent: u64,
		newname: &OsStr,
		_flags: u32,
		reply: ReplyEmpty,
	) {
		trace!("rename");
		futures_executor::block_on(async move {
			let (Ok(from_name), Ok(to_name)) = (name.as_bytes().try_into(), newname.as_bytes().try_into())
				else { return reply.error(libc::ENAMETOOLONG) };

			let from_d = self.ino.get_dir(&self.fs, parent);
			let to_d = self.ino.get_dir(&self.fs, newparent);

			if from_d.transfer(from_name, &to_d, to_name).await.unwrap() {
				reply.ok();
			} else {
				// TODO can also be because dir is not empty or something.
				reply.error(libc::ENOENT);
			}
		})
	}

	fn unlink(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
		trace!("unlink");
		futures_executor::block_on(async move {
			let d = self.ino.get_dir(&self.fs, parent);

			let Ok(name) = name.as_bytes().try_into() else { return reply.error(libc::ENAMETOOLONG) };
			if d.remove(name).await.unwrap() {
				reply.ok()
			} else {
				// TODO can also be because dir is not empty or something.
				reply.error(libc::ENOENT)
			}
			self.fs.finish_transaction().await.unwrap();
		});
	}

	fn rmdir(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
		trace!("rmdir");
		futures_executor::block_on(async move {
			let d = self.ino.get_dir(&self.fs, parent);
			let Ok(name) = name.as_bytes().try_into() else { return reply.error(libc::ENAMETOOLONG) };
			if d.remove(name).await.unwrap() {
				reply.ok();
				self.fs.finish_transaction().await.unwrap();
			} else {
				// TODO can also be because dir is not empty or something.
				reply.error(libc::ENOENT);
			}
		})
	}

	fn destroy(&mut self) {
		trace!("destroy");
		futures_executor::block_on(self.fs.finish_transaction()).unwrap();
	}
}

fn mtime_now() -> nrfs::dir::ext::mtime::Entry {
	mtime_sys(SystemTime::now())
}

fn mtime_sys(t: SystemTime) -> nrfs::dir::ext::mtime::Entry {
	nrfs::dir::ext::mtime::Entry {
		mtime: t.duration_since(UNIX_EPOCH).map_or_else(
			|t| -t.duration().as_millis().try_into().unwrap_or(i64::MAX),
			|t| t.as_millis().try_into().unwrap_or(i64::MAX),
		),
	}
}
