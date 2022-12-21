#![forbid(unused_must_use)]
#![forbid(rust_2018_idioms)]

mod inode;

use {
	fuser::*,
	inode::InodeStore,
	log::trace,
	nrfs::{
		dev::FileDev,
		dir::{Entry, EntryData, RemoveError},
		DirOptions, DirRef, Name, Nrfs, RawDirRef, RawFileRef, RawRef, RawSymRef,
	},
	std::{
		ffi::OsStr,
		fs,
		os::unix::ffi::OsStrExt,
		path::Path,
		time::{Duration, SystemTime, UNIX_EPOCH},
	},
};

const TTL: Duration = Duration::MAX;

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

#[derive(Debug)]
struct Fs {
	/// The filesystem.
	fs: Nrfs<FileDev>,
	/// Bidirectional reference and inode mapping.
	ino: InodeStore,
	/// Directory to put "dangling" files in etc.
	fuse_dir: RawDirRef,
}

impl Fs {
	async fn new(io: fs::File) -> Self {
		let global_cache_size = 1 << 24;
		let dirty_cache_size = 1 << 23;

		let fs = FileDev::new(io, nrfs::BlockSize::K4);
		let fs = Nrfs::load([fs].into(), global_cache_size, dirty_cache_size)
			.await
			.unwrap();

		// Ensure the "/FUSE/" directory exists
		let root = fs.root_dir().await.unwrap();
		let fuse_dir = root
			.create_dir(
				b"/FUSE/".into(),
				&DirOptions::new(&[0; 16]),
				&Default::default(),
			)
			.await
			.unwrap()
			.unwrap()
			.into_raw();

		// Add root dir now so it's always at ino 1.
		let mut ino = InodeStore::new(unsafe { libc::getuid() }, unsafe { libc::getgid() });
		ino.add_dir(root, true);

		Self { fs, ino, fuse_dir }
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

	/// Remove dangling files or symbolic links.
	async fn remove_dangling(&self, entry: Entry<'_, FileDev>) {
		trace!("remove_dangling");
		// Remove entry.
		let data = entry.data().await.unwrap();
		let name = entry.key(&data).await.unwrap();
		trace!("  name: {:?}", &name);
		let parent = entry.parent().expect("entry has no parent");
		drop(entry);
		parent
			.remove(&name)
			.await
			.unwrap()
			.expect("failed to remove entry");
	}

	/// Remove a file or symbolic link.
	async fn remove_file(&mut self, parent: u64, name: &Name) -> Result<(), i32> {
		let d = self.ino.get_dir(&self.fs, parent);

		// Be a good UNIX citizen and check the type.
		enum Ty {
			File(RawFileRef),
			Sym(RawSymRef),
		}
		let raw_ref = match d.find(name).await.unwrap() {
			None => return Err(libc::ENOENT),
			Some(Entry::Dir(_)) => return Err(libc::EISDIR),
			Some(Entry::File(f)) => Ty::File(f.as_raw()),
			Some(Entry::Sym(f)) => Ty::Sym(f.as_raw()),
			Some(Entry::Unknown(_)) => return Err(libc::EPERM),
		};

		// First try to remove the entry straight away.
		match d.remove(name).await.unwrap() {
			Ok(()) => Ok(()),
			Err(RemoveError::NotFound) => Err(libc::ENOENT),
			// Shouldn't happen ever but w/e
			Err(RemoveError::NotEmpty) => Err(libc::ENOTEMPTY),
			Err(RemoveError::LiveReference) => {
				// Defer removal
				match raw_ref {
					Ty::File(f) => self.ino.mark_remove_file(f),
					Ty::Sym(f) => self.ino.mark_remove_sym(f),
				}

				// Move file
				// May fail if the file somehow goes poof so don't panic
				let from = self.ino.get_dir(&self.fs, parent);
				let to = self.fuse_dir.into_tmp(&self.fs);
				let to_name = &mtime_now().mtime.to_le_bytes();
				let to_name = to_name.into();
				match from.transfer(name, &to, to_name).await.unwrap() {
					true => {}
					false => {}
				}

				Ok(())
			}
			Err(RemoveError::UnknownType) => Err(libc::EPERM),
		}
	}
}

impl Filesystem for Fs {
	fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), i32> {
		use fuser::consts::*;
		const CAP: u32 =
			FUSE_ASYNC_READ | FUSE_BIG_WRITES | FUSE_WRITE_CACHE | FUSE_NO_OPEN_SUPPORT;
		config.add_capabilities(CAP).unwrap();
		Ok(())
	}

	fn lookup(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
		futures_executor::block_on(async move {
			let d = self.ino.get_dir(&self.fs, parent);

			let Ok(name) = name.as_bytes().try_into() else { return reply.error(libc::ENAMETOOLONG) };
			let Some(entry) = d.find(name).await.unwrap() else { return reply.error(libc::ENOENT) };

			let data = entry.data().await.unwrap();

			// Get type, len, add to inode store
			let (ty, len, ino) = match entry {
				Entry::Dir(d) => {
					let len = d.len().await.unwrap().into();
					let ino = self.ino.add_dir(d, true);
					(FileType::Directory, len, ino)
				}
				Entry::File(f) => {
					let len = f.len().await.unwrap();
					let ino = self.ino.add_file(f, true);
					(FileType::RegularFile, len, ino)
				}
				Entry::Sym(f) => {
					let len = f.len().await.unwrap();
					let ino = self.ino.add_sym(f, true);
					(FileType::Symlink, len, ino)
				}
				Entry::Unknown(_) => todo!("unknown entry type"),
			};

			reply.entry(&TTL, &self.attr(ino, ty, len, &data), 0)
		})
	}

	fn forget(&mut self, _req: &Request<'_>, ino: u64, nlookup: u64) {
		futures_executor::block_on(async move {
			if let Some(entry) = self.ino.forget(&self.fs, ino, nlookup) {
				self.remove_dangling(entry).await;
			}
		})
	}

	fn getattr(&mut self, _: &Request<'_>, ino: u64, reply: ReplyAttr) {
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

			reply.attr(&TTL, &self.attr(ino, ty, size, &data));
		})
	}

	fn read(
		&mut self,
		_req: &Request<'_>,
		ino: u64,
		_fh: u64,
		offset: i64,
		size: u32,
		_flags: i32,
		_lock: Option<u64>,
		reply: ReplyData,
	) {
		futures_executor::block_on(async move {
			let mut buf = vec![0; size as _];
			let f = self.ino.get_file(&self.fs, ino);
			let l = f.read(offset as _, &mut buf).await.unwrap();
			reply.data(&buf[..l]);
		})
	}

	fn write(
		&mut self,
		_req: &Request<'_>,
		ino: u64,
		_fh: u64,
		offset: i64,
		data: &[u8],
		_write_flags: u32,
		_flags: i32,
		_lock_owner: Option<u64>,
		reply: ReplyWrite,
	) {
		futures_executor::block_on(async move {
			let f = self.ino.get_file(&self.fs, ino);

			f.write_grow(offset as _, data).await.unwrap();
			reply.written(data.len() as _);
		})
	}

	fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
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

				if name.starts_with(b"/") {
					// Internal name (e.g. "/FUSE/"), skip
					index = i;
					continue;
				}

				let (ty, e_ino) = match e {
					Entry::Dir(d) => (FileType::Directory, self.ino.add_dir(d, false)),
					Entry::File(f) => (FileType::RegularFile, self.ino.add_file(f, false)),
					Entry::Sym(f) => (FileType::Symlink, self.ino.add_sym(f, false)),
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
		req: &Request<'_>,
		parent: u64,
		name: &OsStr,
		mode: u32,
		_umask: u32,
		_flags: i32,
		reply: ReplyCreate,
	) {
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
			let ino = self.ino.add_file(f, false);
			let data = self.ino.get(&self.fs, ino).data().await.unwrap();
			reply.created(
				&TTL,
				&self.attr(ino, FileType::RegularFile, 0, &data),
				0,
				0,
				0,
			);
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
				let ino = self.ino.add_sym(f, false);
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
			let opt = nrfs::DirOptions {
				extensions: *nrfs::dir::EnableExtensions::default()
					.add_unix()
					.add_mtime(),
				..nrfs::dir::DirOptions::new(&[0; 16]) // FIXME randomize
			};
			if let Some(dd) = d.create_dir(name, &opt, &ext).await.unwrap() {
				let ino = self.ino.add_dir(dd, false);
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
		futures_executor::block_on(async move {
			let (Ok(from_name), Ok(to_name)) = (name.as_bytes().try_into(), newname.as_bytes().try_into())
				else { return reply.error(libc::ENAMETOOLONG) };

			// FIXME for gods sake do it properly.

			// Delete entry at original location first.
			if let Err(e) = self.remove_file(newparent, to_name).await {
				if e != libc::ENOENT {
					reply.error(e);
					return;
				}
			}

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

	/// Unlink a file or directory,
	/// i.e. remove it from the directory but keep it alive until all references
	/// to it are gone.
	fn unlink(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
		futures_executor::block_on(async move {
			let Ok(name) = name.as_bytes().try_into() else { return reply.error(libc::ENAMETOOLONG) };
			match self.remove_file(parent, name).await {
				Ok(()) => reply.ok(),
				Err(e) => reply.error(e),
			}
		});
	}

	fn rmdir(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
		futures_executor::block_on(async move {
			let d = self.ino.get_dir(&self.fs, parent);
			let Ok(name) = name.as_bytes().try_into() else { return reply.error(libc::ENAMETOOLONG) };

			// Ensure it's a directory because POSIX yadayada
			let raw_ref = match d.find(name).await.unwrap() {
				Some(Entry::Dir(d)) => d.as_raw(),
				Some(_) => return reply.error(libc::ENOTDIR),
				None => return reply.error(libc::ENOENT),
			};

			match d.remove(name).await.unwrap() {
				Ok(()) => reply.ok(),
				Err(RemoveError::NotFound) => reply.error(libc::ENOENT),
				Err(RemoveError::NotEmpty) => reply.error(libc::ENOTEMPTY),
				Err(RemoveError::LiveReference) => {
					// Defer removal
					self.ino.mark_remove_dir(raw_ref);

					// Move dir
					// May fail if the file somehow goes poof so don't panic
					let from = self.ino.get_dir(&self.fs, parent);
					let to = self.fuse_dir.into_tmp(&self.fs);
					let to_name = &mtime_now().mtime.to_le_bytes();
					let to_name = to_name.into();
					match from.transfer(name, &to, to_name).await.unwrap() {
						true => {}
						false => {}
					}

					reply.ok();
				}
				Err(RemoveError::UnknownType) => reply.error(libc::ENOTDIR),
			}
		})
	}

	fn fsync(&mut self, _: &Request<'_>, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
		futures_executor::block_on(self.fs.finish_transaction()).unwrap();
		reply.ok();
	}

	fn fsyncdir(
		&mut self,
		_: &Request<'_>,
		_ino: u64,
		_fh: u64,
		_datasync: bool,
		reply: ReplyEmpty,
	) {
		futures_executor::block_on(self.fs.finish_transaction()).unwrap();
		reply.ok();
	}

	fn destroy(&mut self) {
		futures_executor::block_on(async move {
			// Remove /FUSE/ directory to keep things clean, which *should* succeed
			drop(DirRef::from_raw(&self.fs, self.fuse_dir.clone()));
			let root = self.fs.root_dir().await.unwrap();
			root.remove(b"/FUSE/".into()).await.unwrap().unwrap();

			self.fs.finish_transaction().await.unwrap();
		})
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
