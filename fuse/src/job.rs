use {
	fuser::{
		ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyStatfs,
		ReplyWrite, TimeOrNow,
	},
	std::{ffi::OsStr, path::Path, time::Instant},
};

/// A job to be completed by the filesystem handler.
#[derive(Debug)]
pub enum Job {
	Lookup(Lookup),
	Forget(Forget),
	GetAttr(GetAttr),
	SetAttr(SetAttr),
	Read(Read),
	Write(Write),
	ReadLink(ReadLink),
	ReadDir(ReadDir),
	Create(Create),
	FAllocate(FAllocate),
	SymLink(SymLink),
	MkDir(MkDir),
	Rename(Rename),
	Unlink(Unlink),
	RmDir(RmDir),
	FSync(FSync),
	StatFs(StatFs),
	Destroy,
	Sync(Instant),
}

#[derive(Debug)]
pub struct Lookup {
	pub parent: u64,
	pub name: Box<OsStr>,
	pub reply: ReplyEntry,
}

#[derive(Debug)]
pub struct Forget {
	pub ino: u64,
	pub nlookup: u64,
}

#[derive(Debug)]
pub struct GetAttr {
	pub ino: u64,
	pub reply: ReplyAttr,
}

#[derive(Debug)]
pub struct SetAttr {
	pub ino: u64,
	pub mode: Option<u32>,
	pub uid: Option<u32>,
	pub gid: Option<u32>,
	pub size: Option<u64>,
	pub mtime: Option<TimeOrNow>,
	pub reply: ReplyAttr,
}

#[derive(Debug)]
pub struct Read {
	pub ino: u64,
	pub offset: i64,
	pub size: u32,
	pub reply: ReplyData,
}

#[derive(Debug)]
pub struct Write {
	pub ino: u64,
	pub offset: i64,
	pub data: Box<[u8]>,
	pub reply: ReplyWrite,
}

#[derive(Debug)]
pub struct ReadLink {
	pub ino: u64,
	pub reply: ReplyData,
}

#[derive(Debug)]
pub struct ReadDir {
	pub ino: u64,
	pub offset: i64,
	pub reply: ReplyDirectory,
}

#[derive(Debug)]
pub struct Create {
	pub uid: u32,
	pub gid: u32,
	pub parent: u64,
	pub name: Box<OsStr>,
	pub mode: u32,
	pub reply: ReplyCreate,
}

#[derive(Debug)]
pub struct FAllocate {
	pub ino: u64,
	pub length: i64,
	pub reply: ReplyEmpty,
}

#[derive(Debug)]
pub struct SymLink {
	pub uid: u32,
	pub gid: u32,
	pub parent: u64,
	pub name: Box<OsStr>,
	pub link: Box<Path>,
	pub reply: ReplyEntry,
}

#[derive(Debug)]
pub struct MkDir {
	pub uid: u32,
	pub gid: u32,
	pub parent: u64,
	pub name: Box<OsStr>,
	pub mode: u32,
	pub reply: ReplyEntry,
}

#[derive(Debug)]
pub struct Rename {
	pub parent: u64,
	pub name: Box<OsStr>,
	pub newparent: u64,
	pub newname: Box<OsStr>,
	pub reply: ReplyEmpty,
}

#[derive(Debug)]
pub struct Unlink {
	pub parent: u64,
	pub name: Box<OsStr>,
	pub reply: ReplyEmpty,
}

#[derive(Debug)]
pub struct RmDir {
	pub parent: u64,
	pub name: Box<OsStr>,
	pub reply: ReplyEmpty,
}

#[derive(Debug)]
pub struct FSync {
	pub reply: ReplyEmpty,
}

#[derive(Debug)]
pub struct StatFs {
	pub reply: ReplyStatfs,
}
