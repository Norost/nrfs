mod channel;
mod inode;
mod ops;

pub use channel::FsChannel;

use {
	crate::job::Job,
	async_channel::{self, Receiver},
	fuser::*,
	inode::InodeStore,
	nrfs::{
		dev::FileDev,
		dir::{ItemData, ItemRef, RemoveError},
		Background, Name, Nrfs,
	},
	std::{
		cell::RefCell,
		fs,
		time::{Duration, SystemTime, UNIX_EPOCH},
	},
};

const TTL: Duration = Duration::MAX;

/// Filesystem wrapper.
#[derive(Debug)]
pub struct Fs {
	/// The filesystem.
	fs: Nrfs<FileDev>,
	/// Bidirectional reference and inode mapping.
	ino: RefCell<InodeStore>,
	/// Receiver for jobs from FUSE session handler.
	channel: Receiver<Job>,
}

impl Fs {
	pub async fn new(
		permissions: u16,
		io: impl Iterator<Item = fs::File>,
		key: Option<[u8; 32]>,
	) -> (Self, FsChannel) {
		let retrieve_key = &mut |use_password| {
			if let Some(key) = key {
				Some(nrfs::KeyPassword::Key(key))
			} else if use_password {
				let pwd = rpassword::prompt_password("Password: ").expect("failed to ask password");
				Some(nrfs::KeyPassword::Password(pwd.into_bytes()))
			} else {
				None
			}
		};

		let devices = io.map(|f| FileDev::new(f, nrfs::BlockSize::K4)).collect();
		let conf =
			nrfs::LoadConfig { retrieve_key, devices, cache_size: 1 << 24, allow_repair: true };
		eprintln!("Mounting filesystem");
		let fs = Nrfs::load(conf).await.unwrap();

		// Add root dir now so it's always at ino 1.
		let mut ino = InodeStore::new(permissions, unsafe { libc::getuid() }, unsafe {
			libc::getgid()
		});

		let bg = Background::default();
		let root = bg.run(fs.root_dir(&bg)).await.unwrap();
		ino.add_dir(root, true);
		bg.drop().await.unwrap();

		let (send, recv) = async_channel::bounded(1024);

		(
			Self { fs, ino: ino.into(), channel: recv },
			FsChannel { channel: send },
		)
	}

	pub async fn run(self) -> Result<(), nrfs::Error<FileDev>> {
		eprintln!("Running");
		let bg = Background::default();
		bg.run(async {
			loop {
				let job = self.channel.recv().await.unwrap();
				macro_rules! switch {
					{ [$bg:ident $job:ident] $($v:ident $f:ident)* } => {
						match $job {
							$(Job::$v(job) => self.$f(&$bg, job).await,)*
							Job::Destroy => {
								self.destroy(&bg).await;
								break;
							}
						}
					};
				}
				switch! {
					[bg job]
					Lookup lookup
					Forget forget
					GetAttr getattr
					SetAttr setattr
					Read read
					Write write
					ReadLink readlink
					ReadDir readdir
					Create create
					FAllocate fallocate
					SymLink symlink
					MkDir mkdir
					Rename rename
					Unlink unlink
					RmDir rmdir
					FSync fsync
					StatFs statfs
				}
			}
			Ok::<_, nrfs::Error<_>>(())
		})
		.await?;
		eprintln!("Session closed, unmounting");
		bg.drop().await?;
		self.fs.unmount().await?;
		Ok(())
	}

	/// Convert [`ItemData`] et al. to [`FileAttr`].
	fn attr(&self, ino: u64, ty: FileType, len: u64, data: &ItemData) -> FileAttr {
		let self_ino = self.ino.borrow_mut();

		let u = data.ext_unix.unwrap_or(self_ino.unix_default);

		let mtime = data.ext_mtime.map_or(UNIX_EPOCH, |t| {
			if t.mtime > 0 {
				UNIX_EPOCH.checked_add(Duration::from_micros(t.mtime as _))
			} else {
				UNIX_EPOCH.checked_sub(Duration::from_micros(-i128::from(t.mtime) as _))
			}
			.unwrap()
		});

		let blksize = 1u32 << self.fs.block_size().to_raw();

		// "Number of 512B blocks allocated"
		let blocks =
			u64::try_from((u128::from(len) + u128::from(blksize) - 1) / u128::from(blksize))
				.unwrap();
		let blocks = blocks << (self.fs.block_size().to_raw() - 9);

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
			blocks,
			ino,
			blksize,
		}
	}

	/// Remove a file or symbolic link.
	async fn remove_file<'a>(
		&'a self,
		bg: &Background<'a, FileDev>,
		parent: u64,
		name: &Name,
	) -> Result<(), i32> {
		let self_ino = self.ino.borrow_mut();

		let d = self_ino.get_dir(&self.fs, bg, parent);

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

fn mtime_now() -> nrfs::dir::ext::mtime::Entry {
	mtime_sys(SystemTime::now())
}

fn mtime_sys(t: SystemTime) -> nrfs::dir::ext::mtime::Entry {
	nrfs::dir::ext::mtime::Entry {
		mtime: t.duration_since(UNIX_EPOCH).map_or_else(
			|t| -t.duration().as_micros().try_into().unwrap_or(i64::MAX),
			|t| t.as_micros().try_into().unwrap_or(i64::MAX),
		),
	}
}
