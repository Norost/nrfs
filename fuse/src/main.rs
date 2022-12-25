#![forbid(unused_must_use)]
#![forbid(rust_2018_idioms)]

mod inode;

use {
	fuser::*,
	inode::InodeStore,
	log::trace,
	nrfs::{
		dev::FileDev,
		dir::{InsertError, ItemData, ItemRef, RemoveError, TransferError},
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
}

impl Fs {
	async fn new(io: fs::File) -> Self {
		let global_cache_size = 1 << 24;
		let dirty_cache_size = 1 << 23;

		let fs = FileDev::new(io, nrfs::BlockSize::K4);
		let fs = Nrfs::load([fs].into(), global_cache_size, dirty_cache_size, false)
			.await
			.unwrap();

		// Add root dir now so it's always at ino 1.
		let mut ino = InodeStore::new(unsafe { libc::getuid() }, unsafe { libc::getgid() });
		let root = fs.root_dir().await.unwrap();
		ino.add_dir(root, true);

		Self { fs, ino }
	}

	/// Convert [`ItemData`] et al. to [`FileAttr`].
	fn attr(&self, ino: u64, ty: FileType, len: u64, data: &ItemData) -> FileAttr {
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

	/// Remove a file or symbolic link.
	async fn remove_file(&mut self, parent: u64, name: &Name) -> Result<(), i32> {
		let d = self.ino.get_dir(&self.fs, parent);

		// Be a good UNIX citizen and check the type.
		let Some(e) = d.find(name).await.unwrap() else { return Err(libc::ENOENT) };
		let r = match &e {
			ItemRef::Dir(_) => Err(libc::EISDIR),
			ItemRef::File(_) | ItemRef::Sym(_) => Ok(()),
			ItemRef::Unknown(_) => Err(libc::EPERM),
		};
		e.drop().await.unwrap();
		r?;

		// First try to remove the entry straight away.
		match d.remove(name).await.unwrap() {
			Ok(()) => Ok(()),
			Err(RemoveError::NotFound) => Err(libc::ENOENT),
			// Shouldn't happen ever but w/e
			Err(RemoveError::NotEmpty) => Err(libc::ENOTEMPTY),
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
				ItemRef::Dir(d) => {
					let len = d.len().await.unwrap().into();
					let (ino, e) = self.ino.add_dir(d, true);
					if let Some(e) = e {
						e.drop().await.unwrap()
					}
					(FileType::Directory, len, ino)
				}
				ItemRef::File(f) => {
					let len = f.len().await.unwrap();
					let (ino, e) = self.ino.add_file(f, true);
					if let Some(e) = e {
						e.drop().await.unwrap()
					}
					(FileType::RegularFile, len, ino)
				}
				ItemRef::Sym(f) => {
					let len = f.len().await.unwrap();
					let (ino, e) = self.ino.add_sym(f, true);
					if let Some(e) = e {
						e.drop().await.unwrap()
					}
					(FileType::Symlink, len, ino)
				}
				ItemRef::Unknown(_) => todo!("unknown entry type"),
			};

			reply.entry(&TTL, &self.attr(ino, ty, len, &data), 0)
		})
	}

	fn forget(&mut self, _req: &Request<'_>, ino: u64, nlookup: u64) {
		if let Some(r) = self.ino.forget(&self.fs, ino, nlookup) {
			futures_executor::block_on(r.drop()).unwrap();
		}
	}

	fn getattr(&mut self, _: &Request<'_>, ino: u64, reply: ReplyAttr) {
		futures_executor::block_on(async move {
			let entry = self.ino.get(&self.fs, ino);

			// Get type, len
			let (ty, len) = match &*entry {
				ItemRef::Dir(d) => (FileType::Directory, d.len().await.unwrap().into()),
				ItemRef::File(f) => (FileType::RegularFile, f.len().await.unwrap()),
				ItemRef::Sym(f) => (FileType::Symlink, f.len().await.unwrap()),
				ItemRef::Unknown(_) => unreachable!(),
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
				ItemRef::Dir(d) => (FileType::Directory, d.len().await.unwrap().into()),
				ItemRef::File(f) => {
					let len = if let Some(size) = size {
						f.resize(size).await.unwrap();
						size
					} else {
						f.len().await.unwrap()
					};
					(FileType::RegularFile, len)
				}
				ItemRef::Sym(f) => {
					let len = if let Some(size) = size {
						f.resize(size).await.unwrap();
						size
					} else {
						f.len().await.unwrap()
					};
					(FileType::Symlink, len)
				}
				ItemRef::Unknown(_) => unreachable!(),
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

			let mut index = offset as u64 - 2;
			while let Some((e, i)) = d.next_from(index).await.unwrap() {
				let data = e.data().await.unwrap();
				let Some(name) = e.key(&data).await.unwrap() else {
					// Entry may have been removed just after we fetched it,
					// so just skip.
					e.drop().await.unwrap();
					index = i;
					continue;
				};

				let (ty, e_ino) = match e {
					ItemRef::Dir(d) => {
						let (ino, e) = self.ino.add_dir(d, false);
						if let Some(e) = e {
							e.drop().await.unwrap()
						}
						(FileType::Directory, ino)
					}
					ItemRef::File(f) => {
						let (ino, e) = self.ino.add_file(f, false);
						if let Some(e) = e {
							e.drop().await.unwrap()
						}
						(FileType::RegularFile, ino)
					}
					ItemRef::Sym(f) => {
						let (ino, e) = self.ino.add_sym(f, false);
						if let Some(e) = e {
							e.drop().await.unwrap()
						}
						(FileType::Symlink, ino)
					}
					ItemRef::Unknown(_) => todo!("miscellaneous file type"),
				};
				d = self.ino.get_dir(&self.fs, ino);

				let offt = i as i64 + 2;
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
			match d.create_file(name, &ext).await.unwrap() {
				Ok(f) => {
					let (ino, f) = self.ino.add_file(f, false);
					if let Some(f) = f {
						f.drop().await.unwrap()
					}
					let data = self.ino.get(&self.fs, ino).data().await.unwrap();
					reply.created(
						&TTL,
						&self.attr(ino, FileType::RegularFile, 0, &data),
						0,
						0,
						0,
					);
				}
				Err(InsertError::Duplicate) => reply.error(libc::EEXIST),
				// This is what Linux's tmpfs returns.
				Err(InsertError::Dangling) => reply.error(libc::ENOENT),
				Err(InsertError::Full) => todo!("figure out error code"),
			}
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
				ItemRef::Dir(_) => reply.error(libc::EISDIR),
				ItemRef::File(f) => f.resize(length as _).await.unwrap(),
				ItemRef::Sym(f) => f.resize(length as _).await.unwrap(),
				ItemRef::Unknown(_) => unreachable!(),
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
			match d.create_sym(name, &ext).await.unwrap() {
				Ok(f) => {
					let link = link.as_os_str().as_bytes();
					f.write_grow(0, link).await.unwrap();
					let (ino, f) = self.ino.add_sym(f, false);
					if let Some(f) = f {
						f.drop().await.unwrap()
					}
					let data = self.ino.get(&self.fs, ino).data().await.unwrap();
					let attr = self.attr(ino, FileType::Symlink, link.len() as _, &data);
					reply.entry(&TTL, &attr, 0);
				}
				Err(InsertError::Duplicate) => reply.error(libc::EEXIST),
				// This is what Linux's tmpfs returns.
				Err(InsertError::Dangling) => reply.error(libc::ENOENT),
				Err(InsertError::Full) => todo!("figure out error code"),
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
			match d.create_dir(name, &opt, &ext).await.unwrap() {
				Ok(dd) => {
					let (ino, dd) = self.ino.add_dir(dd, false);
					if let Some(dd) = dd {
						dd.drop().await.unwrap()
					}
					let data = self.ino.get(&self.fs, ino).data().await.unwrap();
					let attr = self.attr(ino, FileType::Directory, 0, &data);
					reply.entry(&TTL, &attr, 0);
				}
				Err(InsertError::Duplicate) => reply.error(libc::EEXIST),
				// This is what Linux's tmpfs returns.
				Err(InsertError::Dangling) => reply.error(libc::ENOENT),
				Err(InsertError::Full) => todo!("figure out error code"),
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

			match from_d.transfer(from_name, &to_d, to_name).await.unwrap() {
				Ok(()) => reply.ok(),
				Err(TransferError::NotFound) => reply.error(libc::ENOENT),
				// On Linux existing entries are overwritten.
				Err(TransferError::Duplicate) => todo!("existing entry should have been removed"),
				Err(TransferError::IsAncestor) => reply.error(libc::EINVAL),
				Err(TransferError::Full) => todo!("figure error code for full dir"),
				// This is what Linux returns if you try to create an entry in an unlinked dir.
				Err(TransferError::Dangling) => reply.error(libc::ENOENT),
				Err(TransferError::UnknownType) => todo!("figure out error code for unknown type"),
			}
		})
	}

	/// Unlink a file or symbolic link,
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
			let Some(e) = d.find(name).await.unwrap() else { return reply.error(libc::ENOENT) };
			let r = match &e {
				ItemRef::Dir(_) => Ok(()),
				_ => Err(libc::ENOTDIR),
			};
			e.drop().await.unwrap();
			if let Err(e) = r {
				return reply.error(e);
			};

			match d.remove(name).await.unwrap() {
				Ok(()) => reply.ok(),
				Err(RemoveError::NotFound) => reply.error(libc::ENOENT),
				Err(RemoveError::NotEmpty) => reply.error(libc::ENOTEMPTY),
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
			self.ino.remove_all(&self.fs).await;
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
