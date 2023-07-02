use fuser::ReplyIoctl;

use {
	crate::job::*,
	async_channel::Sender,
	fuser::{
		Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
		ReplyEntry, ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow,
	},
	std::{
		ffi::OsStr,
		os::unix::ffi::OsStrExt,
		path::Path,
		time::{Duration, Instant, SystemTime},
	},
};

/// Channel to communicate between FUSE session handler and the filesystem handler.
#[derive(Clone)]
pub struct FsChannel {
	pub(super) channel: Sender<Job>,
}

impl FsChannel {
	fn send(&self, job: Job) {
		self.channel.send_blocking(job).unwrap()
	}

	pub fn commit(&mut self) -> bool {
		let when = Instant::now().checked_add(Duration::from_secs(1)).unwrap();
		self.channel.send_blocking(Job::Sync(when)).is_ok()
	}
}

impl Filesystem for FsChannel {
	fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), i32> {
		use fuser::consts::*;
		const CAP: u32 = FUSE_ASYNC_READ
			| FUSE_BIG_WRITES
			| FUSE_WRITEBACK_CACHE
			| FUSE_NO_OPEN_SUPPORT
			| FUSE_AUTO_INVAL_DATA
			| FUSE_CACHE_SYMLINKS
			| FUSE_PARALLEL_DIROPS
			| FUSE_NO_OPENDIR_SUPPORT
			| FUSE_HAS_IOCTL_DIR;
		config.add_capabilities(CAP).unwrap();
		if let Err(m) = config.set_max_write(1 << 24) {
			config.set_max_write(m).unwrap();
		}
		if let Err(m) = config.set_max_readahead(1 << 24) {
			config.set_max_readahead(m).unwrap();
		}
		Ok(())
	}

	fn lookup(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
		self.send(Job::Lookup(Lookup {
			parent,
			name: name.as_bytes().into(),
			reply,
		}));
	}

	fn forget(&mut self, _: &Request<'_>, ino: u64, nlookup: u64) {
		self.send(Job::Forget(Forget { ino, nlookup }));
	}

	fn getattr(&mut self, _: &Request<'_>, ino: u64, reply: ReplyAttr) {
		self.send(Job::GetAttr(GetAttr { ino, reply }));
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
		self.send(Job::SetAttr(SetAttr {
			ino,
			mode,
			uid,
			gid,
			size,
			mtime,
			reply,
		}));
	}

	fn getxattr(&mut self, _: &Request<'_>, ino: u64, name: &OsStr, size: u32, reply: ReplyXattr) {
		self.send(Job::GetXAttr(GetXAttr {
			ino,
			name: name.as_bytes().into(),
			size,
			reply,
		}));
	}

	fn setxattr(
		&mut self,
		_: &Request<'_>,
		ino: u64,
		name: &OsStr,
		value: &[u8],
		flags: i32,
		position: u32,
		reply: ReplyEmpty,
	) {
		self.send(Job::SetXAttr(SetXAttr {
			ino,
			name: name.as_bytes().into(),
			value: value.into(),
			flags,
			position,
			reply,
		}));
	}

	fn listxattr(&mut self, _: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
		self.send(Job::ListXAttr(ListXAttr { ino, size, reply }));
	}

	fn removexattr(&mut self, _: &Request<'_>, ino: u64, name: &OsStr, reply: ReplyEmpty) {
		self.send(Job::RemoveXAttr(RemoveXAttr {
			ino,
			name: name.as_bytes().into(),
			reply,
		}));
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
		self.send(Job::Read(Read { ino, offset, size, reply }));
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
		self.send(Job::Write(Write { ino, offset, data: data.into(), reply }));
	}

	fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
		self.send(Job::ReadLink(ReadLink { ino, reply }));
	}

	fn readdir(
		&mut self,
		_req: &Request<'_>,
		ino: u64,
		_fh: u64,
		offset: i64,
		reply: ReplyDirectory,
	) {
		self.send(Job::ReadDir(ReadDir { ino, offset, reply }));
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
		self.send(Job::Create(Create {
			uid: req.uid(),
			gid: req.gid(),
			parent,
			name: name.as_bytes().into(),
			mode,
			reply,
		}));
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
		self.send(Job::FAllocate(FAllocate { ino, length, reply }));
	}

	fn symlink(
		&mut self,
		req: &Request<'_>,
		parent: u64,
		name: &OsStr,
		link: &Path,
		reply: ReplyEntry,
	) {
		self.send(Job::SymLink(SymLink {
			uid: req.uid(),
			gid: req.gid(),
			parent,
			name: name.as_bytes().into(),
			link: link.as_os_str().as_bytes().into(),
			reply,
		}));
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
		self.send(Job::MkDir(MkDir {
			uid: req.uid(),
			gid: req.gid(),
			parent,
			name: name.as_bytes().into(),
			mode,
			reply,
		}));
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
		self.send(Job::Rename(Rename {
			parent,
			name: name.as_bytes().into(),
			newparent,
			newname: newname.as_bytes().into(),
			reply,
		}));
	}

	/// Unlink a file or symbolic link,
	fn unlink(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
		self.send(Job::Unlink(Unlink {
			parent,
			name: name.as_bytes().into(),
			reply,
		}));
	}

	fn rmdir(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
		self.send(Job::RmDir(RmDir {
			parent,
			name: name.as_bytes().into(),
			reply,
		}));
	}

	fn fsync(&mut self, _: &Request<'_>, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
		self.send(Job::FSync(FSync { reply }));
	}

	fn fsyncdir(
		&mut self,
		_: &Request<'_>,
		_ino: u64,
		_fh: u64,
		_datasync: bool,
		reply: ReplyEmpty,
	) {
		self.send(Job::FSync(FSync { reply }));
	}

	fn statfs(&mut self, _: &Request<'_>, _: u64, reply: ReplyStatfs) {
		self.send(Job::StatFs(StatFs { reply }))
	}

	fn ioctl(
		&mut self,
		_: &Request<'_>,
		ino: u64,
		_fh: u64,
		flags: u32,
		cmd: u32,
		in_data: &[u8],
		out_size: u32,
		reply: ReplyIoctl,
	) {
		self.send(Job::IoCtl(IoCtl {
			ino,
			flags,
			cmd,
			in_data: in_data.into(),
			out_size,
			reply,
		}))
	}

	fn mknod(
		&mut self,
		req: &Request<'_>,
		parent: u64,
		name: &OsStr,
		mode: u32,
		umask: u32,
		rdev: u32,
		reply: ReplyEntry,
	) {
		self.send(Job::MkNod(MkNod {
			uid: req.uid(),
			gid: req.gid(),
			parent,
			name: name.as_bytes().into(),
			mode,
			umask,
			rdev,
			reply,
		}))
	}

	fn destroy(&mut self) {
		self.send(Job::Destroy);
	}
}
