mod create;
mod destroy;
mod fallocate;
mod forget;
mod fsync;
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
	super::{mtime_now, mtime_sys, Fs, TTL},
	fuser::{FileType, TimeOrNow},
	nrfs::dir::{InsertError, ItemRef, RemoveError, TransferError},
	std::os::unix::ffi::OsStrExt,
};

/// [Apparently inodes by `readdir` (and `getattr`?) are ignored *by user applications*][1].
///
/// `-1` (equivalent to `u64::MAX`) apparently indicates "unknown inode".
///
/// [1]: https://sourceforge.net/p/fuse/mailman/fuse-devel/thread/CAOw_e7ZGpmYuFpL6ajQV%3DyRFgw7tdo70BU%3D1CW-jfJABDgPG8w%40mail.gmail.com/
/// [2]: https://x.cygwin.com/ml/cygwin/2006-01/msg00982.html
const NO_INO: u64 = u64::MAX;
