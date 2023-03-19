mod channel;
mod inode;
mod ops;

pub use channel::FsChannel;

use {
	crate::job::Job,
	async_channel::{self, Receiver},
	fuser::*,
	inode::InodeStore,
	nrfs::{dev::FileDev, ItemExt, ItemKey, MTime, Name, Nrfs},
	std::{
		cell::{RefCell, RefMut},
		fs,
		future::Future,
		pin::Pin,
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

		let devices = io.map(|f| FileDev::new(f)).collect();
		let conf = nrfs::LoadConfig { retrieve_key, devices, cache_size, allow_repair: true };
		eprintln!("Mounting filesystem");
		let fs = Nrfs::load(conf).await.unwrap();

		// Add root dir now so it's always at ino 1.
		let mut ino = InodeStore::new(permissions, unsafe { libc::getuid() }, unsafe {
			libc::getgid()
		});

		ino.add_dir(fs.root_dir().into_key());

		let (send, recv) = async_channel::bounded(1024);

		(
			Self { fs, ino: ino.into(), channel: recv },
			FsChannel { channel: send },
		)
	}

	pub async fn run(self) -> Result<(), nrfs::Error<FileDev>> {
		eprintln!("Running");
		self.fs
			.run(async {
				loop {
					let job = self.channel.recv().await.unwrap();
					macro_rules! switch {
					{ [$job:ident] $($v:ident $f:ident)* } => {
						match $job {
							$(Job::$v(job) => self.$f(job).await,)*
							Job::Destroy => {
								self.destroy().await;
								break;
							}
						}
					};
				}
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
						FSync fsync
						StatFs statfs
					}
				}
				Ok::<_, nrfs::Error<_>>(())
			})
			.await?;
		eprintln!("Session closed, unmounting");
		self.fs.unmount().await?;
		Ok(())
	}

	/// Convert [`ItemData`] et al. to [`FileAttr`].
	fn attr(&self, ino: u64, ty: FileType, len: u64, ext: ItemExt) -> FileAttr {
		let unix = ext.unix.unwrap_or(self.ino().unix_default);
		let mtime = ext.mtime.map_or(UNIX_EPOCH, |t| {
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
			perm: unix.permissions,
			nlink: 1,
			uid: unix.uid(),
			gid: unix.gid(),
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
	async fn remove_file<'a>(&'a self, parent: u64, name: &Name) -> Result<(), i32> {
		let parent = self.ino().get_dir(parent);
		let parent = self.fs.dir(parent);

		let Some(item) = parent.search(name).await.unwrap()
			else { return Err(libc::ENOENT) };
		let file = match item.key() {
			ItemKey::File(f) | ItemKey::Sym(f) => f,
			ItemKey::Dir(_) => return Err(libc::EISDIR),
		};
		let file = self.fs.file(file);

		let ino = self.ino().get_ino(item.key());
		if let Some(ino) = ino {
			self.fs.item(item.key()).erase_name().await.unwrap();
			self.ino().mark_unlinked(ino);
		} else {
			file.destroy().await.unwrap();
		}
		Ok(())
	}

	/// Clean up a dangling item if it is not referenced.
	async fn clean_dangling(&self, item: ItemKey) {
		if self.ino().get_ino(item).is_some() {
			return;
		}
		match item {
			ItemKey::Dir(d) => {
				let d = self.fs.dir(d);
				let mut index = 0;
				let mut in_use = false;
				while let Some((info, i)) = d.next_from(index).await.unwrap() {
					if self.ino().get_ino(info.key()).is_some() {
						in_use = true;
					} else {
						let fut = box_fut(self.clean_dangling(info.key()));
						fut.await;
					}
					index = i;
				}
				if !in_use {
					d.destroy().await.unwrap().unwrap();
				}
			}
			ItemKey::File(f) | ItemKey::Sym(f) => {
				self.fs.file(f).destroy().await.unwrap();
			}
		}
	}

	#[track_caller]
	fn ino(&self) -> RefMut<'_, InodeStore> {
		self.ino.borrow_mut()
	}
}

fn mtime_now() -> MTime {
	mtime_sys(SystemTime::now())
}

fn mtime_sys(t: SystemTime) -> MTime {
	MTime {
		mtime: t.duration_since(UNIX_EPOCH).map_or_else(
			|t| -t.duration().as_micros().try_into().unwrap_or(i64::MAX >> 1),
			|t| t.as_micros().try_into().unwrap_or(i64::MAX >> 1),
		),
	}
}

fn box_fut<'a, F: Future + 'a>(fut: F) -> Pin<Box<dyn Future<Output = F::Output> + 'a>> {
	Box::pin(fut)
}
