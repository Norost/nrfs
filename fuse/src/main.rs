use {
	arena::{Arena, Handle},
	fuser::*,
	log::{debug, *},
	nrfs::{Name, Storage},
	std::{
		collections::HashMap,
		ffi::OsStr,
		fs,
		hash::{BuildHasher, Hash, Hasher},
		io::{self, Read, Seek, SeekFrom, Write},
		os::unix::ffi::OsStrExt,
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
	fuser::mount2(
		Fs::new(f),
		m,
		&[
			MountOption::FSName("nrfs".into()),
			MountOption::DefaultPermissions,
		],
	)?;
	Ok(())
}

struct Fs {
	sto: nrfs::Nrfs<S>,
	ino: InodeStore,
}

#[derive(Default)]
struct InodeStore {
	/// Inode to directory with permissions, GUID etc.
	dir: Arena<(Dir, u64), ()>,
	/// Inode to directory ID + file map
	file: Arena<(File, u64), ()>,
	/// Inode to directory ID + sym map
	sym: Arena<(File, u64), ()>,
	/// Reverse lookup from directory ID
	dir_rev: HashMap<u64, Handle<()>>,
	/// Reverse lookup from directory ID + file
	file_rev: HashMap<(u64, Rc<Name>), Handle<()>>,
	/// Reverse lookup from directory ID + sym
	sym_rev: HashMap<(u64, Rc<Name>), Handle<()>>,
	/// Default UID
	uid: u32,
	/// Default GID
	gid: u32,
}

struct Dir {
	id: u64,
	unix: nrfs::dir::ext::unix::Entry,
}

struct File {
	dir: u64,
	name: Rc<Name>,
	unix: nrfs::dir::ext::unix::Entry,
}

enum Inode<D, F, S> {
	Dir(D),
	File(F),
	Sym(S),
}

impl Fs {
	fn new(io: fs::File) -> Self {
		let sto = nrfs::Nrfs::load(S::new(io)).unwrap();
		let uid = unsafe { libc::getuid() };
		let gid = unsafe { libc::getgid() };
		let mut s = Self { sto, ino: InodeStore { uid, gid, ..Default::default() } };
		s.ino.add_dir(
			Dir { id: 0, unix: nrfs::dir::ext::unix::Entry { permissions: 0o777, uid, gid } },
			true,
		);
		s
	}

	fn attr(&self, ty: FileType, size: u64, ino: u64) -> FileAttr {
		let u = match self.ino.get(ino) {
			Inode::Dir(d) => &d.unix,
			Inode::File(f) | Inode::Sym(f) => &f.unix,
		};
		let blksize = 1u32 << self.sto.storage().block_size_p2();
		FileAttr {
			atime: UNIX_EPOCH,
			mtime: UNIX_EPOCH,
			ctime: UNIX_EPOCH,
			crtime: UNIX_EPOCH,
			perm: u.permissions,
			nlink: 1,
			uid: u.uid,
			gid: u.gid,
			rdev: 0,
			flags: 0,
			kind: ty,
			size,
			blocks: (size + u64::from(blksize) - 1) / u64::from(blksize),
			ino,
			blksize,
		}
	}
}

impl InodeStore {
	fn add_dir(&mut self, dir: Dir, incr: bool) -> u64 {
		Self::add(&mut self.dir, &mut self.dir_rev, dir.id, dir, incr) | INO_TY_DIR
	}

	fn add_file(&mut self, file: File, incr: bool) -> u64 {
		let k = (file.dir, file.name.clone());
		Self::add(&mut self.file, &mut self.file_rev, k, file, incr) | INO_TY_FILE
	}

	fn add_sym(&mut self, sym: File, incr: bool) -> u64 {
		let k = (sym.dir, sym.name.clone());
		Self::add(&mut self.sym, &mut self.sym_rev, k, sym, incr) | INO_TY_SYM
	}

	fn add<T, K>(
		m: &mut Arena<(T, u64), ()>,
		rev_m: &mut HashMap<K, Handle<()>>,
		k: K,
		t: T,
		incr: bool,
	) -> u64
	where
		K: Hash + Eq,
	{
		let h = if let Some(h) = rev_m.get_mut(&k) {
			m[*h].1 += u64::from(incr);
			*h
		} else {
			let h = m.insert((t, 0));
			rev_m.insert(k, h);
			h
		};
		// Because ROOT_ID is reserved for the root dir, but nrfs uses 0 for the root dir
		h.into_raw().0 as u64 + 1
	}

	fn get(&self, ino: u64) -> Inode<&Dir, &File, &File> {
		let h = Handle::from_raw((ino & !INO_TY_MASK) as usize - 1, ());
		match ino & INO_TY_MASK {
			INO_TY_DIR => Inode::Dir(&self.dir[h].0),
			INO_TY_FILE => Inode::File(&self.file[h].0),
			INO_TY_SYM => Inode::Sym(&self.sym[h].0),
			_ => unreachable!(),
		}
	}

	fn get_dir(&self, ino: u64) -> &Dir {
		&self.dir[Handle::from_raw((ino ^ INO_TY_DIR) as usize - 1, ())].0
	}

	fn get_file(&self, ino: u64) -> &File {
		&self.file[Handle::from_raw((ino ^ INO_TY_FILE) as usize - 1, ())].0
	}

	fn get_sym(&self, ino: u64) -> &File {
		&self.sym[Handle::from_raw((ino ^ INO_TY_SYM) as usize - 1, ())].0
	}

	fn forget(&mut self, ino: u64, nlookup: u64) {
		let h = Handle::from_raw((ino & !INO_TY_MASK) as usize - 1, ());
		match ino & INO_TY_MASK {
			INO_TY_DIR => {
				let c = &mut self.dir[h].1;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					debug!("remove dir {:?}", h);
					let (d, _) = self.dir.remove(h).unwrap();
					self.dir_rev.remove(&d.id);
				}
			}
			INO_TY_FILE => {
				let c = &mut self.file[h].1;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					debug!("remove file {:?}", h);
					let (f, _) = self.file.remove(h).unwrap();
					self.file_rev.remove(&(f.dir, f.name));
				}
			}
			INO_TY_SYM => {
				let c = &mut self.sym[h].1;
				*c = c.saturating_sub(nlookup);
				if *c == 0 {
					debug!("remove sym {:?}", h);
					let (f, _) = self.sym.remove(h).unwrap();
					self.sym_rev.remove(&(f.dir, f.name));
				}
			}
			_ => unreachable!(),
		}
	}

	fn get_unix(&self, entry: &nrfs::dir::Entry<'_, '_, S>) -> nrfs::dir::ext::unix::Entry {
		entry
			.ext_unix()
			.copied()
			.unwrap_or(nrfs::dir::ext::unix::Entry {
				permissions: 0o700,
				uid: self.uid,
				gid: self.gid,
			})
	}
}

impl Filesystem for Fs {
	fn lookup(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
		let d = self.ino.get_dir(parent);
		let mut d = self.sto.get_dir(d.id).unwrap();
		match name
			.as_bytes()
			.try_into()
			.ok()
			.and_then(|n| d.find(n).unwrap())
		{
			Some(mut e) if e.is_dir() => {
				let d = e.as_dir().unwrap().unwrap();
				let l = d.len().into();
				let ino = self
					.ino
					.add_dir(Dir { id: d.id(), unix: self.ino.get_unix(&e) }, true);
				reply.entry(&TTL, &self.attr(FileType::Directory, l, ino), 0)
			}
			Some(mut e) if e.is_file() => {
				let l = e.as_file().unwrap().len().unwrap();
				let ino = self.ino.add_file(
					File { name: e.name().into(), unix: self.ino.get_unix(&e), dir: d.id() },
					true,
				);
				reply.entry(&TTL, &self.attr(FileType::RegularFile, l, ino), 0)
			}
			Some(mut e) if e.is_sym() => {
				let l = e.as_sym().unwrap().len().unwrap();
				let ino = self.ino.add_sym(
					File { name: e.name().into(), unix: self.ino.get_unix(&e), dir: d.id() },
					true,
				);
				reply.entry(&TTL, &self.attr(FileType::Symlink, l, ino), 0)
			}
			Some(_) => todo!(),
			None => reply.error(libc::ENOENT),
		}
	}

	fn forget(&mut self, _req: &Request<'_>, ino: u64, nlookup: u64) {
		self.ino.forget(ino, nlookup)
	}

	fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
		match self.ino.get(ino) {
			Inode::Dir(d) => {
				let d = self.sto.get_dir(d.id).unwrap();
				let l = d.len().into();
				reply.attr(&TTL, &self.attr(FileType::Directory, l, ino));
			}
			Inode::File(f) | Inode::Sym(f) => {
				let mut d = self.sto.get_dir(f.dir).unwrap();
				let mut e = d.find(&f.name).unwrap().unwrap();
				let l = e.as_file().unwrap().len().unwrap();
				let ty = if e.is_file() {
					FileType::RegularFile
				} else {
					FileType::Symlink
				};
				reply.attr(&TTL, &self.attr(ty, l, ino));
			}
		}
	}

	fn setattr(
		&mut self,
		_req: &Request<'_>,
		ino: u64,
		mode: Option<u32>,
		_uid: Option<u32>,
		_gid: Option<u32>,
		size: Option<u64>,
		_atime: Option<TimeOrNow>,
		_mtime: Option<TimeOrNow>,
		_ctime: Option<SystemTime>,
		_fh: Option<u64>,
		_crtime: Option<SystemTime>,
		_chgtime: Option<SystemTime>,
		_bkuptime: Option<SystemTime>,
		_flags: Option<u32>,
		reply: ReplyAttr,
	) {
		let (ty, size) = match self.ino.get(ino) {
			Inode::Dir(d) => {
				let d = self.sto.get_dir(d.id).unwrap();
				(FileType::Directory, d.len().into())
			}
			Inode::File(f) => {
				let mut d = self.sto.get_dir(f.dir).unwrap();
				let mut e = d.find(&f.name).unwrap().unwrap();
				let mut f = e.as_file().unwrap();
				size.map(|s| f.resize(s).unwrap());
				(FileType::RegularFile, f.len().unwrap())
			}
			Inode::Sym(s) => {
				let mut d = self.sto.get_dir(s.dir).unwrap();
				let mut e = d.find(&s.name).unwrap().unwrap();
				let mut s = e.as_file().unwrap();
				(FileType::RegularFile, s.len().unwrap())
			}
		};
		reply.attr(&TTL, &self.attr(ty, size, ino));
	}

	fn read(
		&mut self,
		req: &Request,
		ino: u64,
		_fh: u64,
		offset: i64,
		size: u32,
		_flags: i32,
		_lock: Option<u64>,
		reply: ReplyData,
	) {
		let mut buf = vec![0; size as _];
		let f = self.ino.get_file(ino);
		let mut d = self.sto.get_dir(f.dir).unwrap();
		let mut e = d.find(&f.name).unwrap().unwrap();
		let mut f = e.as_file().unwrap();
		let l = f.read(offset as _, &mut buf).unwrap();
		reply.data(&buf[..l]);
	}

	fn write(
		&mut self,
		req: &Request,
		ino: u64,
		_fh: u64,
		offset: i64,
		data: &[u8],
		_write_flags: u32,
		_flags: i32,
		_lock_owner: Option<u64>,
		reply: ReplyWrite,
	) {
		let f = self.ino.get_file(ino);
		let mut d = self.sto.get_dir(f.dir).unwrap();
		let mut e = d.find(&f.name).unwrap().unwrap();
		let mut f = e.as_file().unwrap();
		f.write_grow(offset as _, data).unwrap();
		reply.written(data.len() as _);

		self.sto.finish_transaction().unwrap();
	}

	fn readlink(&mut self, req: &Request, ino: u64, reply: ReplyData) {
		let mut buf = [0; 1 << 15];
		let f = self.ino.get_sym(ino);
		let mut d = self.sto.get_dir(f.dir).unwrap();
		let mut e = d.find(&f.name).unwrap().unwrap();
		let mut f = e.as_sym().unwrap();
		let l = f.read(0, &mut buf).unwrap();
		reply.data(&buf[..l]);
	}

	fn readdir(
		&mut self,
		req: &Request<'_>,
		ino: u64,
		_fh: u64,
		mut offset: i64,
		mut reply: ReplyDirectory,
	) {
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

		let d = self.ino.get_dir(ino);
		let mut d = self.sto.get_dir(d.id).unwrap();
		let d_id = d.id();

		let mut index = Some(offset as u32 - 2);
		while let Some((mut e, i)) = index.and_then(|i| d.next_from(i).unwrap()) {
			let unix = self.ino.get_unix(&e);
			let (ty, ino) = if let Some(id) = e.dir_id() {
				(
					FileType::Directory,
					self.ino.add_dir(Dir { id, unix }, false),
				)
			} else if e.as_file().is_some() {
				(
					FileType::RegularFile,
					self.ino
						.add_file(File { dir: d_id, name: e.name().into(), unix }, false),
				)
			} else if e.as_sym().is_some() {
				(
					FileType::Symlink,
					self.ino
						.add_sym(File { dir: d_id, name: e.name().into(), unix }, false),
				)
			} else {
				unreachable!("miscellaneous file type");
			};
			let offt = i.map(|i| i64::from(i) + 2).unwrap_or(i64::MAX);
			if reply.add(ino, offt, ty, OsStr::from_bytes(e.name())) {
				break;
			}
			index = i;
		}

		reply.ok();
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
		let d = self.ino.get_dir(parent);
		let mut d = self.sto.get_dir(d.id).unwrap();

		if let Ok(name) = name.as_bytes().try_into() {
			d.create_file(name, &Default::default()).unwrap();
			let ino = self.ino.add_file(
				File {
					dir: d.id(),
					name: name.into(),
					unix: nrfs::dir::ext::unix::Entry {
						permissions: mode as _,
						uid: req.uid(),
						gid: req.gid(),
					},
				},
				false,
			);
			reply.created(&TTL, &self.attr(FileType::RegularFile, 0, ino), 0, 0, 0);
			self.sto.finish_transaction().unwrap();
		} else {
			reply.error(libc::ENAMETOOLONG);
		}
	}

	fn fallocate(
		&mut self,
		req: &Request<'_>,
		ino: u64,
		_fh: u64,
		_offset: i64,
		length: i64,
		_mode: i32,
		reply: ReplyEmpty,
	) {
		match self.ino.get(ino) {
			Inode::Dir(_) => reply.error(libc::EISDIR),
			Inode::File(f) | Inode::Sym(f) => {
				let mut d = self.sto.get_dir(f.dir).unwrap();
				let mut e = d.find(&f.name).unwrap().unwrap();
				let mut f = e.as_file().unwrap();
				f.resize(length as _).unwrap();
				self.sto.finish_transaction().unwrap();
				reply.ok();
			}
		}
	}

	fn unlink(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
		let d = self.ino.get_dir(parent);
		let mut d = self.sto.get_dir(d.id).unwrap();

		if let Ok(name) = name.as_bytes().try_into() {
			if d.remove(name).unwrap() {
				reply.ok()
			} else {
				reply.error(libc::ENOENT)
			}
		} else {
			reply.error(libc::ENAMETOOLONG);
		}
		self.sto.finish_transaction().unwrap();
	}

	fn rmdir(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
		let d = self.ino.get_dir(parent);
		let mut d = self.sto.get_dir(d.id).unwrap();
		// TODO use let_else as soon as Rust 1.65 is stable
		if let Ok(name) = name.as_bytes().try_into() {
			if let Some(mut e) = d.find(name).unwrap() {
				if let Some(d) = e.as_dir().map(|d| d.unwrap()) {
					if d.len() == 0 {
						e.remove().unwrap();
						reply.ok()
					} else {
						reply.error(libc::ENOTEMPTY);
					}
				} else {
					reply.error(libc::ENOTDIR);
				}
			} else {
				reply.error(libc::ENOENT);
			}
		} else {
			reply.error(libc::ENAMETOOLONG);
		}
		self.sto.finish_transaction().unwrap();
	}

	fn destroy(&mut self) {
		self.sto.finish_transaction().unwrap();
	}
}

#[derive(Debug)]
struct S {
	file: fs::File,
	block_count: u64,
}

impl S {
	fn new(file: fs::File) -> Self {
		Self { block_count: file.metadata().unwrap().len() >> 9, file }
	}
}

impl nrfs::Storage for S {
	type Error = io::Error;

	fn block_size_p2(&self) -> u8 {
		9
	}

	fn block_count(&self) -> u64 {
		self.block_count
	}

	fn read(&mut self, lba: u64, blocks: usize) -> Result<Box<dyn nrfs::Read + '_>, Self::Error> {
		self.file
			.seek(SeekFrom::Start(lba << self.block_size_p2()))?;
		let mut buf = vec![0; blocks << self.block_size_p2()];
		self.file.read_exact(&mut buf)?;
		Ok(Box::new(R { buf }))
	}

	fn write(&mut self, lba: u64, blocks: usize) -> Result<Box<dyn nrfs::Write + '_>, Self::Error> {
		let bsp2 = self.block_size_p2();
		let buf = vec![0; blocks << bsp2];
		Ok(Box::new(W { s: self, offset: lba << bsp2, buf }))
	}

	fn fence(&mut self) -> Result<(), Self::Error> {
		self.file.flush()?;
		self.file.sync_all()
	}
}

struct R {
	buf: Vec<u8>,
}

impl nrfs::Read for R {
	fn get(&self) -> &[u8] {
		&self.buf
	}
}

struct W<'a> {
	s: &'a mut S,
	offset: u64,
	buf: Vec<u8>,
}

impl<'a> nrfs::Write for W<'a> {
	fn get_mut(&mut self) -> &mut [u8] {
		&mut self.buf
	}

	fn set_blocks(&mut self, blocks: usize) {
		self.buf.resize(blocks << 9, 0);
	}
}

impl Drop for W<'_> {
	fn drop(&mut self) {
		let e = (|| {
			self.s.file.seek(SeekFrom::Start(self.offset))?;
			self.s.file.write_all(&self.buf)
		})();
		if let Err(e) = e {
			error!("write failed: {}", e)
		}
	}
}
