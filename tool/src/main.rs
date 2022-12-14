#![forbid(unused_must_use)]
#![feature(pin_macro)]

#[cfg(target_family = "unix")]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use {
	clap::Parser,
	core::{
		future::Future,
		pin::{pin, Pin},
		ptr,
		task::{Context, RawWaker, RawWakerVTable, Waker},
	},
	nrfs::{dev::FileDev, BlockSize, Nrfs},
	std::{
		fs::{self, File, OpenOptions},
		io::{Read as _, Seek as _, SeekFrom},
		path::{Path, PathBuf},
	},
};

#[derive(Debug, Parser)]
#[clap(
	author = "David Hoppenbrouwers",
	version = "0.1",
	about = "Tool for creating & working with NRFS filesystems"
)]
enum Command {
	Make(Make),
	Dump(Dump),
}

#[derive(Debug, clap::StructOpt)]
#[clap(about = "Create a new filesystem")]
struct Make {
	#[clap(help = "The path to the image to put the filesystem in")]
	path: String,
	#[clap(short, long, help = "The directory to copy to the image")]
	directory: Option<PathBuf>,
	#[clap(
		short,
		long,
		help = "Whether to resolve symlinks when copying a directory"
	)]
	follow: bool,
	#[clap(short, long, default_value_t = 17, help = "The record size to use")]
	record_size_p2: u8,
	#[clap(
		short,
		long,
		help = "The block size to use",
		long_help = "If not specified it is derived automatically\n\
		If derivation fails, it defaults to 12 (4K)"
	)]
	block_size_p2: Option<u8>,
	#[clap(short, long, value_enum, default_value_t = Compression::Lz4, help = "The compression to use")]
	compression: Compression,
}

#[derive(Clone, Debug, clap::ArgEnum)]
enum Compression {
	None,
	Lz4,
}

#[derive(Debug, clap::StructOpt)]
#[clap(about = "Dump the contents of a filesystem")]
struct Dump {
	#[clap(help = "The path to the filesystem image")]
	path: String,
}

// https://github.com/rust-lang/rust/pull/96875/files
const VTABLE: RawWakerVTable = RawWakerVTable::new(|_| RAW, |_| {}, |_| {}, |_| {});
const RAW: RawWaker = RawWaker::new(ptr::null(), &VTABLE);

fn main() {
	let fut = async {
		match Command::parse() {
			Command::Make(args) => make(args).await,
			Command::Dump(args) => dump(args).await,
		}
	};
	let mut fut = pin!(fut);
	let waker = unsafe { Waker::from_raw(RAW) };
	let mut cx = Context::from_waker(&waker);
	while fut.as_mut().poll(&mut cx).is_pending() {}
}

async fn make(args: Make) {
	let f = OpenOptions::new()
		.truncate(false)
		.read(true)
		.write(true)
		.open(&args.path)
		.unwrap();

	let block_size = if let Some(v) = args.block_size_p2 {
		v
	} else {
		#[cfg(target_family = "unix")]
		let bs = f.metadata().unwrap().blksize().trailing_zeros() as _;
		#[cfg(not(target_family = "unix"))]
		let bs = 12;
		bs
	};
	let block_size = BlockSize::from_raw(block_size).unwrap();

	let mut extensions = nrfs::dir::EnableExtensions::default();
	extensions.add_unix();
	extensions.add_mtime();
	let opt = nrfs::DirOptions { extensions, ..Default::default() };
	let rec_size = nrfs::MaxRecordSize::K128; // TODO
	let compr = match args.compression {
		Compression::None => nrfs::Compression::None,
		Compression::Lz4 => nrfs::Compression::Lz4,
	};

	let global_cache_size = 1 << 20;
	let dirty_cache_size = 1 << 20;

	let s = FileDev::new(f, block_size);
	let mut nrfs = Nrfs::new(
		[[s]],
		block_size,
		rec_size,
		&opt,
		compr,
		global_cache_size,
		dirty_cache_size,
	)
	.await
	.unwrap();

	if let Some(d) = &args.directory {
		let mut root = nrfs.root_dir().await.unwrap();
		add_files(&mut root, d, &args, extensions).await;
	}
	//nrfs.finish_transaction().await.unwrap();
	nrfs.unmount().await.unwrap();

	async fn add_files(
		root: &mut nrfs::Dir<'_, FileDev>,
		from: &Path,
		args: &Make,
		extensions: nrfs::dir::EnableExtensions,
	) {
		for f in fs::read_dir(from).expect("failed to read dir") {
			let f = f.unwrap();
			let m = f.metadata().unwrap();
			let n = f.file_name();
			let n = n.to_str().unwrap().try_into().unwrap();

			let mut ext = nrfs::dir::Extensions::default();

			ext.unix = extensions.unix().then(|| {
				let mut u =
					nrfs::dir::ext::unix::Entry { permissions: 0o700, ..Default::default() };
				let p = m.permissions();
				#[cfg(target_family = "unix")]
				{
					u.permissions = (p.mode() & 0o777) as _;
					u.uid = m.uid();
					u.gid = m.gid();
				}
				u
			});

			ext.mtime = extensions.mtime().then(|| nrfs::dir::ext::mtime::Entry {
				mtime: m
					.modified()
					.ok()
					.and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
					.map(|t| t.as_millis().try_into().unwrap_or(i64::MAX))
					.unwrap_or(0),
			});

			if m.is_file() || (m.is_symlink() && args.follow) {
				let c = fs::read(f.path()).unwrap();
				let mut f = root.create_file(n, &ext).await.unwrap().unwrap();
				f.write_grow(0, &c).await.unwrap();
			} else if m.is_dir() {
				let opt = nrfs::DirOptions { extensions, ..Default::default() };
				let mut d = root.create_dir(n, &opt, &ext).await.unwrap().unwrap();
				let path = f.path();
				let fut: Pin<Box<dyn Future<Output = ()>>> =
					Box::pin(add_files(&mut d, &path, args, extensions));
				fut.await
			} else if m.is_symlink() {
				let c = fs::read_link(f.path()).unwrap();
				let mut f = root.create_sym(n, &ext).await.unwrap().unwrap();
				f.write_grow(0, c.to_str().unwrap().as_bytes())
					.await
					.unwrap();
			} else {
				todo!()
			}
		}
	}
}

async fn dump(args: Dump) {
	let global_cache_size = 1 << 20;
	let dirty_cache_size = 1 << 20;

	let mut f = File::open(args.path).unwrap();

	// FIXME block size shouldn't matter.
	let mut block_size_p2 = [0];
	f.seek(SeekFrom::Start(20)).unwrap();
	f.read_exact(&mut block_size_p2).unwrap();

	let s = FileDev::new(f, BlockSize::from_raw(block_size_p2[0]).unwrap());
	let mut nrfs = Nrfs::load([s].into(), global_cache_size, dirty_cache_size)
		.await
		.unwrap();

	let mut root = nrfs.root_dir().await.unwrap();
	println!("block size: 2**{}", block_size_p2[0]);
	list_files(&mut root, 0).await;

	async fn list_files(root: &mut nrfs::Dir<'_, FileDev>, indent: usize) {
		let mut i = Some(0);
		while let Some((mut e, next_i)) = async {
			if let Some(i) = i {
				root.next_from(i).await.unwrap()
			} else {
				None
			}
		}
		.await
		{
			if let Some(u) = e.ext_unix() {
				let mut s = [0; 9];
				for (i, (c, l)) in s.iter_mut().zip(b"rwxrwxrwx").rev().enumerate() {
					*c = [b'-', *l][usize::from(u.permissions & 1 << i != 0)];
				}
				print!(
					"{} {:>4} {:>4}  ",
					std::str::from_utf8(&s).unwrap(),
					u.uid,
					u.gid
				);
			}

			if let Some(t) = e.ext_mtime() {
				let secs = (t.mtime / 1000) as i64;
				let millis = t.mtime.rem_euclid(1000) as u32;
				let t = chrono::NaiveDateTime::from_timestamp(secs, millis * 1_000_000);
				// Use format!() since NaiveDateTime doesn't respect flags
				print!("{:<23}", format!("{}", t));
			}

			let name = String::from_utf8_lossy(e.name()).into_owned();
			if e.is_file() {
				let mut f = e.as_file().unwrap();
				println!(
					"{:>8}  {:>indent$}{}f {}",
					f.len().await.unwrap(),
					"",
					[' ', 'e'][usize::from(e.is_embedded())],
					name,
					indent = indent
				);
			} else if e.is_dir() {
				let mut d = e.as_dir().await.unwrap().unwrap();
				println!(
					"{:>8}  {:>indent$} d {}",
					d.len(),
					"",
					name,
					indent = indent
				);
				let fut: Pin<Box<dyn Future<Output = _>>> =
					Box::pin(list_files(&mut d, indent + 2));
				fut.await;
			} else if e.is_sym() {
				let mut f = e.as_sym().unwrap();
				let len = f.len().await.unwrap();
				let mut buf = vec![0; len as _];
				f.read_exact(0, &mut buf).await.unwrap();
				let link = String::from_utf8_lossy(&buf);
				println!(
					"{:>indent$}{}s {} -> {}",
					"",
					[' ', 'e'][usize::from(e.is_embedded())],
					name,
					link,
					indent = 10 + indent
				);
			}
			i = next_i
		}
	}
}
