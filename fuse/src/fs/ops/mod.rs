mod create;
mod destroy;
mod fallocate;
mod forget;
mod getattr;
mod lookup;
mod mkdir;
mod read;
mod readdir;
mod readlink;
mod rename;
mod rmdir;
mod setattr;
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
	nrfs::{CreateError, Item, ItemTy},
	std::os::unix::ffi::OsStrExt,
};

/// [Apparently inodes by `readdir` (and `getattr`?) are ignored *by user applications*][1].
///
/// `-1` (equivalent to `u64::MAX`) apparently indicates "unknown inode".
///
/// [1]: https://sourceforge.net/p/fuse/mailman/fuse-devel/thread/CAOw_e7ZGpmYuFpL6ajQV%3DyRFgw7tdo70BU%3D1CW-jfJABDgPG8w%40mail.gmail.com/
/// [2]: https://x.cygwin.com/ml/cygwin/2006-01/msg00982.html
const NO_INO: u64 = u64::MAX;

pub(super) struct Attrs {
	pub mtime: Option<i128>,
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

attr!(set b"nrfs.mtime" set_mtime t encode_s i128);
attr!(set b"nrfs.uid" set_uid uid encode_u libc::uid_t);
attr!(set b"nrfs.gid" set_gid gid encode_u libc::gid_t);
attr!(set b"nrfs.unixmode" set_mode mode encode_u u16);

async fn init_attrs(
	item: &Item<'_, Dev>,
	uid: libc::uid_t,
	gid: libc::gid_t,
	mode: Option<u16>,
) -> Attrs {
	let mtime = mtime_now();
	set_mtime(item, mtime).await;
	set_uid(item, uid).await;
	set_gid(item, gid).await;
	if let Some(mode) = mode {
		set_mode(item, mode).await;
	}
	Attrs { mtime: Some(mtime), uid: Some(uid), gid: Some(gid), mode }
}

async fn get_u(item: &Item<'_, Dev>, key: &nrfs::Key) -> Option<u128> {
	item.attr(key).await.unwrap().map(|b| decode_u(&b))
}

async fn get_s(item: &Item<'_, Dev>, key: &nrfs::Key) -> Option<i128> {
	item.attr(key).await.unwrap().map(|b| decode_s(&b))
}

async fn get_attrs(item: &Item<'_, Dev>) -> Attrs {
	let f = |n: u128| n.try_into().unwrap_or(0);
	let g = |n: u128| n.try_into().unwrap_or(0);
	Attrs {
		mtime: get_s(item, b"nrfs.mtime".into()).await,
		uid: get_u(item, b"nrfs.uid".into()).await.map(f),
		gid: get_u(item, b"nrfs.gid".into()).await.map(f),
		mode: get_u(item, b"nrfs.unixmode".into()).await.map(g),
	}
}

fn decode_u(b: &[u8]) -> u128 {
	let mut c = [0; 16];
	c[..b.len()].copy_from_slice(b);
	u128::from_le_bytes(c)
}

fn decode_s(b: &[u8]) -> i128 {
	let mut c = [0; 16];
	if b.last().is_some_and(|&x| x & 0x80 != 0) {
		c.fill(0xff);
	}
	c[..b.len()].copy_from_slice(b);
	i128::from_le_bytes(c)
}

fn encode_u(mut b: &[u8]) -> &[u8] {
	while let Some((&0, c)) = b.split_last() {
		b = c;
	}
	b
}

fn encode_s(mut b: &[u8]) -> &[u8] {
	while let Some(&[x, y]) = b.get(b.len() - 2..) {
		match y {
			0 if x & 0x80 == 0 => b = &b[..b.len() - 1],
			0xff if x & 0x80 != 0 => b = &b[..b.len() - 1],
			_ => break,
		}
	}
	if matches!(b, &[0]) {
		&[]
	} else {
		b
	}
}
