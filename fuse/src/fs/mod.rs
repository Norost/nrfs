mod channel;
mod inode;
mod ops;

pub use channel::FsChannel;

use {
	crate::{dev::Dev, job::Job},
	async_channel::{self, Receiver},
	fuser::*,
	futures_util::{FutureExt, StreamExt},
	inode::InodeStore,
	nrfs::Nrfs,
	std::{
		cell::{RefCell, RefMut},
		fs,
		time::{Duration, SystemTime, UNIX_EPOCH},
	},
};

const TTL: Duration = Duration::MAX;

/// Filesystem wrapper.
#[derive(Debug)]
pub struct Fs {
	/// The filesystem.
	fs: Nrfs<Dev>,
	/// Bidirectional reference and inode mapping.
	ino: RefCell<InodeStore>,
	/// Receiver for jobs from FUSE session handler.
	channel: Receiver<Job>,
	default_uid: libc::uid_t,
	default_gid: libc::gid_t,
	default_mode: u16,
}

impl Fs {
	pub async fn new(
		permissions: u16,
		io: impl Iterator<Item = fs::File>,
		key: Option<[u8; 32]>,
		cache_size: usize,
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

		let devices = io.map(|f| Dev::new(f)).collect();
		let conf = nrfs::LoadConfig { retrieve_key, devices, cache_size, allow_repair: true };
		eprintln!("Mounting filesystem");
		let fs = Nrfs::load(conf).await.unwrap();

		// Add root dir now so it's always at ino 1.
		let mut ino = InodeStore::new();
		ino.add(inode::Key::Dir(fs.root_dir().key()));

		let (send, recv) = async_channel::bounded(1024);

		(
			Self {
				fs,
				ino: ino.into(),
				channel: recv,
				default_uid: unsafe { libc::getuid() },
				default_gid: unsafe { libc::getgid() },
				default_mode: permissions,
			},
			FsChannel { channel: send },
		)
	}

	pub async fn run(self) -> Result<(), nrfs::Error<Dev>> {
		eprintln!("Running");
		self.fs
			.run(async {
				let mut jobs = futures_util::stream::FuturesUnordered::new();
				loop {
					let job = futures_util::select_biased! {
						() = jobs.select_next_some() => continue,
						job = self.channel.recv().fuse() => job.unwrap(),
					};
					macro_rules! switch {
						{ [$job:ident] $($v:ident $f:ident)* } => {
							match $job {
								$(Job::$v(job) => self.$f(job).await,)*
								Job::FSync(_) | Job::Sync(_) | Job::Destroy => unreachable!(),
							}
						};
					}
					match job {
						Job::FSync(fsync) => {
							while let Some(()) = jobs.next().await {}
							self.fs.finish_transaction().await.unwrap();
							fsync.reply.ok();
						}
						Job::Sync(when) => {
							let now = std::time::Instant::now();
							if when >= now {
								while let Some(()) = jobs.next().await {}
								self.fs.finish_transaction().await.unwrap();
							} else {
								eprintln!("Skipping Job::Sync (when: {:?}, now: {:?})", when, now);
							}
						}
						Job::Destroy => break,
						job => jobs.push(async {
							switch! {
								[job]
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
								StatFs statfs
							}
						}),
					}
				}
				while let Some(()) = jobs.next().await {}
				self.destroy().await;
				Ok::<_, nrfs::Error<_>>(())
			})
			.await?;
		eprintln!("Session closed, unmounting");
		self.fs.unmount().await?;
		Ok(())
	}

	/// Convert [`ItemData`] et al. to [`FileAttr`].
	fn attr(&self, ino: u64, ty: FileType, len: u64, attr: ops::Attrs) -> FileAttr {
		let mtime = attr.mtime.unwrap_or(0);
		let uid = attr.uid.unwrap_or(self.default_uid);
		let gid = attr.gid.unwrap_or(self.default_gid);
		let perm = attr.mode.unwrap_or(self.default_mode);

		let mtime = mtime.max(i64::MIN.into()).min(i64::MAX.into());
		let mtime = i64::try_from(mtime).unwrap();
		let mtime = if mtime > 0 {
			UNIX_EPOCH.checked_add(Duration::from_micros(mtime as _))
		} else {
			UNIX_EPOCH.checked_sub(Duration::from_micros(-mtime as _))
		}
		.unwrap();

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
			perm,
			nlink: 1,
			uid,
			gid,
			rdev: 0,
			flags: 0,
			kind: ty,
			size: len,
			blocks,
			ino,
			blksize,
		}
	}

	#[track_caller]
	fn ino(&self) -> RefMut<'_, InodeStore> {
		self.ino.borrow_mut()
	}
}

fn mtime_now() -> i64 {
	mtime_sys(SystemTime::now())
}

fn mtime_sys(t: SystemTime) -> i64 {
	t.duration_since(UNIX_EPOCH).map_or_else(
		|t| -t.duration().as_micros().try_into().unwrap_or(i64::MAX),
		|t| t.as_micros().try_into().unwrap_or(i64::MAX),
	)
}
