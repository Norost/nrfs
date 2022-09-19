use {
	fuser::*,
	nrfs::Storage,
	std::{
		collections::HashMap,
		ffi::OsStr,
		fs::File,
		hash::{BuildHasher, Hasher},
		io::{self, Read, Seek, SeekFrom, Write},
		os::unix::ffi::OsStrExt,
		time::{Duration, UNIX_EPOCH},
	},
};

const TTL: Duration = Duration::MAX;
// Because 1 is reserved for the root dir, but nrfs uses 0 for the root dir
const DIR_INO_OFFSET: u64 = 1;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let mut a = std::env::args().skip(1);
	let f = a.next().ok_or("expected file path")?;
	let m = a.next().ok_or("expected mount path")?;

	fuser::mount2(
		Fs::new(File::open(&f)?),
		m,
		&[MountOption::FSName("nrfs".into())],
	)?;
	Ok(())
}

struct Fs {
	sto: nrfs::Nrfs<S>,
	/// Inode to directory ID + file map
	ino: HashMap<u64, (u64, Box<nrfs::Name>)>,
	ino_counter: u64,
}

impl Fs {
	fn new(io: File) -> Self {
		let sto = nrfs::Nrfs::load(S::new(io)).unwrap();
		Self { sto, ino: Default::default(), ino_counter: 1 << 63 }
	}

	fn attr(&self, ty: FileType, size: u64, ino: u64) -> FileAttr {
		let blksize = 1u32 << self.sto.storage().block_size_p2();
		FileAttr {
			atime: UNIX_EPOCH,
			mtime: UNIX_EPOCH,
			ctime: UNIX_EPOCH,
			crtime: UNIX_EPOCH,
			perm: 0o777,
			nlink: 1,
			uid: 0,
			gid: 0,
			rdev: 0,
			flags: 0,
			kind: ty,
			size,
			blocks: (size + u64::from(blksize) - 1) / u64::from(blksize),
			ino,
			blksize,
		}
	}

	fn add_ino(
		ino: &mut HashMap<u64, (u64, Box<nrfs::Name>)>,
		ino_counter: &mut u64,
		dir: u64,
		file: Box<nrfs::Name>,
	) -> u64 {
		let i = *ino_counter;
		ino.insert(i, (dir, file));
		*ino_counter += 1;
		i
	}

	fn is_dir(&self, ino: u64) -> bool {
		ino & 1 << 63 == 0
	}
}

impl Filesystem for Fs {
	fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
		debug_assert!(parent & 1 << 63 == 0, "parent is not a directory");
		let mut d = self.sto.get_dir(parent - DIR_INO_OFFSET).unwrap();
		match name
			.as_bytes()
			.try_into()
			.ok()
			.and_then(|n| d.find(n).unwrap())
		{
			Some(mut e) if e.is_dir() => {
				let d = e.as_dir().unwrap().unwrap();
				let ino = d.id() + DIR_INO_OFFSET;
				let l = d.len().into();
				reply.entry(&TTL, &self.attr(FileType::Directory, l, ino), 0)
			}
			Some(mut e) if e.is_file() => {
				let l = e.as_file().unwrap().len().unwrap();
				let n = e.name().into();
				let ino = Self::add_ino(&mut self.ino, &mut self.ino_counter, d.id(), n);
				reply.entry(&TTL, &self.attr(FileType::RegularFile, l, ino), 0)
			}
			Some(mut e) if e.is_sym() => {
				let l = e.as_sym().unwrap().len().unwrap();
				let n = e.name().into();
				let ino = Self::add_ino(&mut self.ino, &mut self.ino_counter, d.id(), n);
				reply.entry(&TTL, &self.attr(FileType::Symlink, l, ino), 0)
			}
			Some(_) => todo!(),
			None => reply.error(libc::ENOENT),
		}
	}

	fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
		if self.is_dir(ino) {
			let d = self.sto.get_dir(ino - DIR_INO_OFFSET).unwrap();
			let l = d.len().into();
			reply.attr(&TTL, &self.attr(FileType::Directory, l, ino));
			return;
		}
		let (dir, name) = self.ino.get(&ino).unwrap();
		let mut d = self.sto.get_dir(*dir).unwrap();
		let mut e = d.find(name).unwrap().unwrap();
		let l = e.as_file().unwrap().len().unwrap();
		let ty = if e.is_file() {
			FileType::RegularFile
		} else {
			FileType::Symlink
		};
		reply.attr(&TTL, &self.attr(ty, l, ino));
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
		let mut buf = vec![0; size as _];
		let (dir, name) = self.ino.get(&ino).unwrap();
		let mut d = self.sto.get_dir(*dir).unwrap();
		let mut e = d.find(name).unwrap().unwrap();
		let mut f = e.as_file().unwrap();
		let l = f.read(offset as _, &mut buf).unwrap();
		reply.data(&buf[..l]);
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
		let (dir, name) = self.ino.get(&ino).unwrap();
		let mut d = self.sto.get_dir(*dir).unwrap();
		let mut e = d.find(name).unwrap().unwrap();
		let mut f = e.as_file().unwrap();
		let l = f.write(offset as _, data).unwrap();
		reply.written(l as _);
	}

	fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
		let mut buf = [0; 1 << 15];
		let (dir, name) = self.ino.get(&ino).unwrap();
		let mut d = self.sto.get_dir(*dir).unwrap();
		let mut e = d.find(name).unwrap().unwrap();
		let mut f = e.as_sym().unwrap();
		let l = f.read(0, &mut buf).unwrap();
		reply.data(&buf[..l]);
	}

	fn readdir(
		&mut self,
		_req: &Request<'_>,
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

		let mut d = self.sto.get_dir(ino - DIR_INO_OFFSET).unwrap();
		let d_id = d.id();

		let mut index = Some(offset as u32 - 2);
		while let Some((e, i)) = index.and_then(|i| d.next_from(i).unwrap()) {
			let ty = match 0 {
				_ if e.is_dir() => FileType::Directory,
				_ if e.is_file() => FileType::RegularFile,
				_ if e.is_sym() => FileType::Symlink,
				_ => todo!(),
			};
			let ino = if let Some(i) = e.dir_id() {
				i
			} else {
				Self::add_ino(&mut self.ino, &mut self.ino_counter, d_id, e.name().into())
			};
			if reply.add(
				ino,
				i.map(|i| i64::from(i) + 2).unwrap_or(i64::MAX),
				ty,
				OsStr::from_bytes(e.name()),
			) {
				break;
			}
			index = i;
		}

		reply.ok();
	}

	fn create(
		&mut self,
		_req: &Request,
		parent: u64,
		name: &OsStr,
		_mode: u32,
		_umask: u32,
		_flags: i32,
		reply: ReplyCreate,
	) {
		let dir = parent - DIR_INO_OFFSET;
		if let Ok(name) = name.as_bytes().try_into() {
			let mut d = self.sto.get_dir(dir).unwrap();
			d.create_file(name).unwrap();
			let ino = Self::add_ino(&mut self.ino, &mut self.ino_counter, dir, name.into());
			reply.created(&TTL, &self.attr(FileType::RegularFile, 0, ino), 0, 0, 0);
		} else {
			reply.error(libc::ENAMETOOLONG);
		}
	}
}

#[derive(Debug)]
struct S {
	file: File,
	block_count: u64,
}

impl S {
	fn new(file: File) -> Self {
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
		Ok(())
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
		let _ = (|| {
			self.s.file.seek(SeekFrom::Start(self.offset))?;
			self.s.file.write_all(&self.buf)
		})();
	}
}

struct BuildHashId;

impl BuildHasher for BuildHashId {
	type Hasher = HashId;

	fn build_hasher(&self) -> Self::Hasher {
		HashId(0)
	}
}

struct HashId(u64);

impl Hasher for HashId {
	fn write(&mut self, a: &[u8]) {
		unimplemented!("use write_u64")
	}

	fn write_u64(&mut self, n: u64) {
		self.0 = n;
	}

	fn finish(&self) -> u64 {
		self.0
	}
}
