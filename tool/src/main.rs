use {
	clap::Parser,
	std::{
		fs::{self, File, OpenOptions},
		io::{self, Read as _, Seek as _, SeekFrom, Write as _},
		str::FromStr,
	},
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
	#[clap(value_enum)]
	command: Command,
	file: String,
	#[clap(short)]
	directory: Option<String>,
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
	let mut f = OpenOptions::new()
		.truncate(false)
		.read(true)
		.write(true)
		.open(args.file)
		.unwrap();
	let h = nrfs::header::Header::default();
	f.write_all(h.as_ref()).unwrap();
	let mut nrfs = nrfs::Nrfs::load(S::new(f)).unwrap();

	if let Some(d) = args.directory {
		for f in fs::read_dir(d).expect("failed to read dir") {
			match f {
				Ok(f) if f.metadata().unwrap().is_file() => {
					let f = fs::read(f.path()).unwrap();
					let id = nrfs.new_object().unwrap();
					nrfs.write_object(id, 0, &f).unwrap();
				}
				Ok(_) => {}
				Err(e) => todo!("{:?}", e),
			}
		}
		nrfs.finish_transaction().unwrap();
	}
	dbg!(nrfs);
}

fn dump(args: Args) {
	let f = File::open(args.file).unwrap();
	let mut nrfs = nrfs::Nrfs::load(S::new(f)).unwrap();
	dbg!(&nrfs);
	for id in 0..nrfs.object_count() {
		let len = nrfs.object_len(id).unwrap();
		dbg!(len);
		let mut buf = vec![0; len as _];
		let l = nrfs.read_object(id, 0, &mut buf).unwrap();
		assert_eq!(l, len as usize);
		dbg!();
		std::io::stdout().write_all(&buf).unwrap();
		dbg!();
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
		let buf = Vec::with_capacity(blocks << bsp2);
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
