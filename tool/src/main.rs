use {
	clap::Parser,
	std::{
		fs::{self, File, OpenOptions},
		io::{self, Read as _, Seek as _, SeekFrom, Write as _},
		path::{Path, PathBuf},
	},
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
	#[clap(value_enum)]
	command: Command,
	file: String,
	#[clap(short)]
	directory: Option<PathBuf>,
	#[clap(long)]
	follow: bool,
	#[clap(short, long, default_value_t = 17)]
	record_size_p2: u8,
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum Command {
	Make,
	Dump,
}

fn main() {
	let args = Args::parse();

	match args.command {
		Command::Make => make(args),
		Command::Dump => dump(args),
	}
}

fn make(args: Args) {
	let f = OpenOptions::new()
		.truncate(false)
		.read(true)
		.write(true)
		.open(&args.file)
		.unwrap();

	let mut extensions = nrfs::dir::EnableExtensions::default();
	extensions.add_unix();
	extensions.add_mtime();
	let mut opt = nrfs::DirOptions { extensions, ..Default::default() };
	let rec_size = nrfs::MaxRecordSize::K128; // TODO
	let compr = nrfs::Compression::Lz4;
	let compr = nrfs::Compression::None;
	let mut nrfs = nrfs::Nrfs::new(S::new(f), rec_size, &opt, compr).unwrap();

	if let Some(d) = &args.directory {
		let mut root = nrfs.root_dir().unwrap();
		add_files(&mut root, d, &args, extensions);
	}
	nrfs.finish_transaction().unwrap();

	fn add_files(
		root: &mut nrfs::Dir<'_, S>,
		from: &Path,
		args: &Args,
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
					use std::os::unix::fs::{MetadataExt, PermissionsExt};
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

fn dump(args: Args) {
	let f = File::open(args.file).unwrap();
	let mut nrfs = nrfs::Nrfs::load(S::new(f)).unwrap();
	let mut root = nrfs.root_dir().unwrap();
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
				print!("{:>14?}  ", t);
			}

			let name = String::from_utf8_lossy(e.name()).into_owned();
			if e.is_file() {
				let mut f = e.as_file().unwrap();
				println!(
					"{:>8}  {:>indent$}f {}",
					f.len().unwrap(),
					"",
					name,
					indent = indent
				);
			} else if e.is_dir() {
				let mut d = e.as_dir().unwrap().unwrap();
				println!("{:>8}  {:>indent$}d {}", d.len(), "", name, indent = indent);
				list_files(&mut d, indent + 2);
			} else if e.is_sym() {
				let mut f = e.as_sym().unwrap();
				let len = f.len().unwrap();
				let mut buf = vec![0; len as _];
				f.read_exact(0, &mut buf).unwrap();
				let link = String::from_utf8_lossy(&buf);
				println!(
					"{:>indent$}s {} -> {}",
					"",
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
}

impl S {
	fn new(file: File) -> Self {
		Self { block_count: file.metadata().unwrap().len() >> 9, file }
	}
}

impl nrfs::Storage for S {
	type Error = io::Error;

	fn block_size_p2(&self) -> u8 {
		9
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
		self.offset = lba << 9;
		self.buf.resize(blocks << 9, 0);
		Ok(())
	}

	fn finish(self: Box<Self>) -> Result<(), Self::Error> {
		self.s.file.seek(SeekFrom::Start(self.offset))?;
		self.s.file.write_all(&self.buf)
	}
}
