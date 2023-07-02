mod create;
mod destroy;
mod fallocate;
mod forget;
mod getattr;
mod getxattr;
mod ioctl;
mod listxattr;
mod lookup;
mod mkdir;
mod mknod;
mod read;
mod readdir;
mod readlink;
mod removexattr;
mod rename;
mod rmdir;
mod setattr;
mod setxattr;
mod statfs;
mod symlink;
mod unlink;
mod write;

use {
	super::{
		inode::{Get, Key},
		mtime_now, mtime_sys, Dev, Fs, TTL,
	},
	fuser::{FileType, TimeOrNow},
	nrfs::{CreateError, Item, ItemTy, Modified},
	std::os::unix::ffi::OsStrExt,
	util::task::lock_set::{LockSetExclusiveGuard, LockSetInclusiveGuard},
};

/// [Apparently inodes by `readdir` (and `getattr`?) are ignored *by user applications*][1].
///
/// `-1` (equivalent to `u64::MAX`) apparently indicates "unknown inode".
///
/// [1]: https://sourceforge.net/p/fuse/mailman/fuse-devel/thread/CAOw_e7ZGpmYuFpL6ajQV%3DyRFgw7tdo70BU%3D1CW-jfJABDgPG8w%40mail.gmail.com/
/// [2]: https://x.cygwin.com/ml/cygwin/2006-01/msg00982.html
const NO_INO: u64 = u64::MAX;

const TY_BUILTIN: u16 = 0 << 9;
const TY_BLOCK: u16 = 1 << 9;
const TY_CHAR: u16 = 2 << 9;
const TY_PIPE: u16 = 3 << 9;
const TY_SOCK: u16 = 4 << 9;
#[allow(dead_code)]
const TY_DOOR: u16 = 5 << 9;

pub(super) struct Attrs {
	pub modified: nrfs::Modified,
	pub uid: Option<libc::uid_t>,
	pub gid: Option<libc::gid_t>,
	pub mode: Option<u16>,
}

macro_rules! attr {
	(set $n:literal $f:ident $v:ident $e:ident $t:ty) => {
		async fn $f(item: &Item<'_, Dev>, $v: $t) {
			item.set_attr($n.into(), $e(&$v.to_le_bytes()))
				.await
				.unwrap()
				.unwrap();
		}
	};
}

attr!(set b"nrfs.uid" set_uid uid encode_u libc::uid_t);
attr!(set b"nrfs.gid" set_gid gid encode_u libc::gid_t);
attr!(set b"nrfs.unixmode" set_mode mode encode_u u16);

async fn set_mtime(item: &Item<'_, Dev>, mtime: i64) {
	item.set_modified_time(mtime).await.unwrap();
}

impl Fs {
	async fn init_attrs(
		&self,
		item: &Item<'_, Dev>,
		uid: libc::uid_t,
		gid: libc::gid_t,
		mode: Option<u16>,
	) -> Attrs {
		let modified = Modified { time: mtime_now(), gen: self.gen() };
		let mtime = mtime_now();
		set_mtime(item, mtime).await;
		set_uid(item, uid).await;
		set_gid(item, gid).await;
		if let Some(mode) = mode {
			set_mode(item, mode).await;
		}
		Attrs { modified, uid: Some(uid), gid: Some(gid), mode }
	}

	async fn dir(
		&self,
		ino: u64,
	) -> Result<(nrfs::Dir<'_, Dev>, LockSetInclusiveGuard<'_, u64>), i32> {
		let lock = self.lock(ino).await;
		let dir = match self.ino().get(ino).unwrap() {
			Get::Key(Key::Dir(d), ..) => self.fs.dir(d),
			Get::Key(..) => return Err(libc::ENOTDIR),
			Get::Stale => return Err(libc::ESTALE),
		};
		Ok((dir.await.unwrap(), lock))
	}

	async fn dir_mut(
		&self,
		ino: u64,
	) -> Result<(nrfs::Dir<'_, Dev>, LockSetExclusiveGuard<'_, u64>), i32> {
		let lock = self.lock_mut(ino).await;
		let dir = match self.ino().get(ino).unwrap() {
			Get::Key(Key::Dir(d), ..) => self.fs.dir(d),
			Get::Key(..) => return Err(libc::ENOTDIR),
			Get::Stale => return Err(libc::ESTALE),
		};
		Ok((dir.await.unwrap(), lock))
	}
}

async fn get_u(item: &Item<'_, Dev>, key: &nrfs::Key) -> Option<u128> {
	item.attr(key).await.unwrap().map(|b| decode_u(&b))
}

async fn get_attrs(item: &Item<'_, Dev>) -> Attrs {
	let f = |n: u128| n.try_into().unwrap_or(0);
	let g = |n: u128| n.try_into().unwrap_or(0);
	Attrs {
		modified: item.modified().await.unwrap(),
		uid: get_u(item, b"nrfs.uid".into()).await.map(f),
		gid: get_u(item, b"nrfs.gid".into()).await.map(f),
		mode: get_u(item, b"nrfs.unixmode".into()).await.map(g),
	}
}

fn filter_xattr(key: &[u8]) -> bool {
	key.starts_with(b"nrfs.")
}

fn decode_u(b: &[u8]) -> u128 {
	let mut c = [0; 16];
	c[..b.len()].copy_from_slice(b);
	u128::from_le_bytes(c)
}

fn encode_u(mut b: &[u8]) -> &[u8] {
	while let Some((&0, c)) = b.split_last() {
		b = c;
	}
	b
}

fn getty(mode: u16) -> Option<FileType> {
	Some(match mode & 0o7_000 {
		TY_BUILTIN => FileType::RegularFile,
		TY_CHAR => FileType::CharDevice,
		TY_BLOCK => FileType::BlockDevice,
		TY_PIPE => FileType::NamedPipe,
		TY_SOCK => FileType::Socket,
		_ => return None,
	})
}
