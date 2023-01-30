use {
	crate::job::*,
	async_channel::Sender,
	fuser::{
		Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
		ReplyEntry, ReplyWrite, Request, TimeOrNow,
	},
	std::{ffi::OsStr, path::Path, time::SystemTime},
};

/// Channel to communicate between FUSE session handler and the filesystem handler.
pub struct FsChannel {
	pub(super) channel: Sender<Job>,
}

impl FsChannel {
	fn send(&self, job: Job) {
		self.channel.send_blocking(job).unwrap()
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
			| FUSE_CACHE_SYMLINKS;
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
		self.send(Job::Lookup(Lookup { parent, name: name.into(), reply }));
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
			name: name.into(),
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
			name: name.into(),
			link: link.into(),
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
			name: name.into(),
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
			name: name.into(),
			newparent,
			newname: newname.into(),
			reply,
		}));
	}

	/// Unlink a file or symbolic link,
	fn unlink(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
		self.send(Job::Unlink(Unlink { parent, name: name.into(), reply }));
	}

	fn rmdir(&mut self, _: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
		self.send(Job::RmDir(RmDir { parent, name: name.into(), reply }));
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

	fn destroy(&mut self) {
		self.send(Job::Destroy);
	}
}