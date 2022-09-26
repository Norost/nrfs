#[cfg(target_family = "unix")]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use {
	clap::Parser,
	std::{
		fs::{self, File, OpenOptions},
		io::{self, Read as _, Seek as _, SeekFrom, Write as _},
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

fn main() {
	match Command::parse() {
		Command::Make(args) => make(args),
		Command::Dump(args) => dump(args),
	}
}

fn make(args: Make) {
	let f = OpenOptions::new()
		.truncate(false)
		.read(true)
		.write(true)
		.open(&args.path)
		.unwrap();

	let block_size_p2 = if let Some(v) = args.block_size_p2 {
		v
	} else {
		#[cfg(target_family = "unix")]
		let bs = f.metadata().unwrap().blksize().trailing_zeros() as _;
		#[cfg(not(target_family = "unix"))]
		let bs = 12;
		bs
	};

	let mut extensions = nrfs::dir::EnableExtensions::default();
	extensions.add_unix();
	extensions.add_mtime();
	let opt = nrfs::DirOptions { extensions, ..Default::default() };
	let rec_size = nrfs::MaxRecordSize::K128; // TODO
	let compr = match args.compression {
		Compression::None => nrfs::Compression::None,
		Compression::Lz4 => nrfs::Compression::Lz4,
	};
	let s = S::new(f, block_size_p2);
	let mut nrfs = nrfs::Nrfs::new(s, rec_size, &opt, compr, 32).unwrap();

	if let Some(d) = &args.directory {
		let mut root = nrfs.root_dir().unwrap();
		add_files(&mut root, d, &args, extensions);
	}
	nrfs.finish_transaction().unwrap();

	fn add_files(
		root: &mut nrfs::Dir<'_, S>,
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
				let mut f = root.create_file(n, &ext).unwrap().unwrap();
				f.write_grow(0, &c).unwrap();
			} else if m.is_dir() {
				let opt = nrfs::DirOptions { extensions, ..Default::default() };
				let mut d = root.create_dir(n, &opt, &ext).unwrap().unwrap();
				add_files(&mut d, &f.path(), args, extensions)
			} else if m.is_symlink() {
				let c = fs::read_link(f.path()).unwrap();
				let mut f = root.create_sym(n, &ext).unwrap().unwrap();
				f.write_grow(0, c.to_str().unwrap().as_bytes()).unwrap();
			} else {
				todo!()
			}
		}
	}
}

fn dump(args: Dump) {
	let mut f = File::open(args.path).unwrap();
	let mut block_size_p2 = [0];
	f.seek(SeekFrom::Start(23)).unwrap();
	f.read_exact(&mut block_size_p2).unwrap();
	let s = S::new(f, block_size_p2[0]);
	let mut nrfs = nrfs::Nrfs::load(s, 32).unwrap();
	let mut root = nrfs.root_dir().unwrap();
	println!("block size: 2**{}", block_size_p2[0]);
	list_files(&mut root, 0);

	fn list_files(root: &mut nrfs::Dir<'_, S>, indent: usize) {
		let mut i = Some(0);
		while let Some((mut e, next_i)) = i.and_then(|i| root.next_from(i).unwrap()) {
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
					f.len().unwrap(),
					"",
					[' ', 'e'][usize::from(e.is_embedded())],
					name,
					indent = indent
				);
			} else if e.is_dir() {
				let mut d = e.as_dir().unwrap().unwrap();
				println!(
					"{:>8}  {:>indent$} d {}",
					d.len(),
					"",
					name,
					indent = indent
				);
				list_files(&mut d, indent + 2);
			} else if e.is_sym() {
				let mut f = e.as_sym().unwrap();
				let len = f.len().unwrap();
				let mut buf = vec![0; len as _];
				f.read_exact(0, &mut buf).unwrap();
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

#[derive(Debug)]
struct S {
	file: File,
	block_count: u64,
	block_size_p2: u8,
}

impl S {
	fn new(mut file: File, block_size_p2: u8) -> Self {
		let l = file.seek(SeekFrom::End(0)).unwrap();
		let block_count = l >> block_size_p2;
		Self { block_count, file, block_size_p2 }
	}
}

impl nrfs::Storage for S {
	type Error = io::Error;

	fn block_size_p2(&self) -> u8 {
		self.block_size_p2
	}

	fn block_count(&self) -> u64 {
		self.block_count
	}

	fn read(&mut self, lba: u64, blocks: usize) -> Result<Box<dyn nrfs::Read + '_>, Self::Error> {
		self.file
			.seek(SeekFrom::Start(lba << self.block_size_p2()))?;
		let mut buf = vec![0; blocks << self.block_size_p2()];
		self.file.read_exact(&mut buf)?;
		Ok(Box::new(R { buf }))
	}

	fn write(
		&mut self,
		blocks: usize,
	) -> Result<Box<dyn nrfs::Write<Error = Self::Error> + '_>, Self::Error> {
		let bsp2 = self.block_size_p2();
		let buf = vec![0; blocks << bsp2];
		Ok(Box::new(W { s: self, offset: u64::MAX, buf }))
	}

	fn fence(&mut self) -> Result<(), Self::Error> {
		Ok(())
	}
}

struct R {
	buf: Vec<u8>,
}

impl nrfs::Read for R {
	fn get(&self) -> &[u8] {
		&self.buf
	}
}

struct W<'a> {
	s: &'a mut S,
	offset: u64,
	buf: Vec<u8>,
}

impl<'a> nrfs::Write for W<'a> {
	type Error = io::Error;

	fn get_mut(&mut self) -> &mut [u8] {
		&mut self.buf
	}

	fn set_region(&mut self, lba: u64, blocks: usize) -> Result<(), Self::Error> {
		self.offset = lba << self.s.block_size_p2;
		self.buf.resize(blocks << self.s.block_size_p2, 0);
		Ok(())
	}

	fn finish(self: Box<Self>) -> Result<(), Self::Error> {
		self.s.file.seek(SeekFrom::Start(self.offset))?;
		self.s.file.write_all(&self.buf)
	}
}
