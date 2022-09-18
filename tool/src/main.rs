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
	#[clap(short, long, default_value_t = 17)]
	block_size_p2: u8,
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
		.open(args.file)
		.unwrap();

	let mut nrfs = nrfs::Nrfs::new(S::new(f), args.block_size_p2).unwrap();

	let mut root = nrfs.root_dir().unwrap();
	if let Some(d) = args.directory {
		add_files(&mut root, &d);
		nrfs.finish_transaction().unwrap();
	}

	fn add_files(root: &mut nrfs::Dir<'_, S>, from: &Path) {
		for f in fs::read_dir(from).expect("failed to read dir") {
			let f = f.unwrap();
			let m = f.metadata().unwrap();
			let n = f.file_name();
			let n = n.to_str().unwrap().try_into().unwrap();
			if m.is_file() {
				let c = fs::read(f.path()).unwrap();
				let mut f = root.create_file(n).unwrap().unwrap();
				f.write_all(0, &c).unwrap();
			} else if m.is_dir() {
				let mut d = root.create_dir(n).unwrap().unwrap();
				add_files(&mut d, &f.path())
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
			let name = String::from_utf8_lossy(e.name()).into_owned();
			if e.is_file() {
				let mut f = e.as_file().unwrap();
				println!(
					"{:>8}  {:>indent$}f {:<32}",
					f.len().unwrap(),
					"",
					name,
					indent = indent
				);
			} else if e.is_dir() {
				let mut d = e.as_dir().unwrap().unwrap();
				println!(
					"{:>8}  {:>indent$}d {:<32}",
					d.len(),
					"",
					name,
					indent = indent
				);
				list_files(&mut d, indent + 2);
			} else if e.is_sym() {
				println!("{:>indent$}s {:<32}", "", name, indent = 10 + indent);
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

	fn write(&mut self, lba: u64, blocks: usize) -> Result<Box<dyn nrfs::Write + '_>, Self::Error> {
		let bsp2 = self.block_size_p2();
		let buf = vec![0; blocks << bsp2];
		Ok(Box::new(W { s: self, offset: lba << bsp2, buf }))
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
	fn get_mut(&mut self) -> &mut [u8] {
		&mut self.buf
	}

	fn set_blocks(&mut self, blocks: usize) {
		self.buf.resize(blocks << 9, 0);
	}
}

impl Drop for W<'_> {
	fn drop(&mut self) {
		let _ = (|| {
			self.s.file.seek(SeekFrom::Start(self.offset))?;
			self.s.file.write_all(&self.buf)
		})();
	}
}
