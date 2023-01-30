#![forbid(unused_must_use)]
#![forbid(rust_2018_idioms)]
#![feature(pin_macro)]

#[cfg(target_family = "unix")]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use {
	clap::Parser,
	core::{future::Future, pin::Pin},
	nrfs::{dev::FileDev, dir::ItemRef, BlockSize, CipherType, Nrfs},
	std::{
		fs::{self, File, OpenOptions},
		path::{Path, PathBuf},
	},
};

#[derive(Debug, clap::Parser)]
#[clap(
	author = "David Hoppenbrouwers",
	version = "0.1",
	about = "Tool for creating & working with NRFS filesystems"
)]
enum Command {
	Make(Make),
	Dump(Dump),
}

#[derive(Debug, clap::Args)]
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
	#[clap(
		short,
		long,
		value_enum,
		default_value = "lz4",
		help = "The compression to use"
	)]
	compression: Compression,
	#[clap(short, long, value_enum, help = "Encryption to use on the filesystem")]
	encryption: Option<Encryption>,
	#[clap(
		short,
		long,
		value_enum,
		default_value = "argon2id",
		help = "Which algorithm to use to derive the key for encryption",
		long_help = "If none, a 32-byte key must be supplied"
	)]
	key_derivation_function: KeyDerivationFunction,
	#[clap(long, default_value_t = 1 << 27, help = "Soft limit on the cache")]
	cache_size: usize,
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum Compression {
	None,
	Lz4,
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum Encryption {
	Chacha8Poly1305,
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum KeyDerivationFunction {
	None,
	Argon2id,
}

#[derive(Debug, clap::Args)]
#[clap(about = "Dump the contents of a filesystem")]
struct Dump {
	#[clap(help = "The path to the filesystem image")]
	path: String,
	#[clap(long, default_value_t = 1 << 27, help = "Soft limit on the global cache size")]
	cache_size: usize,
}

fn main() {
	let fut = async {
		match Command::parse() {
			Command::Make(args) => make(args).await,
			Command::Dump(args) => dump(args).await,
		}
	};
	futures_executor::block_on(fut);
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
	// FIXME randomize key
	let opt = nrfs::DirOptions { extensions, ..nrfs::DirOptions::new(&[0; 16]) };
	let rec_size = nrfs::MaxRecordSize::K128; // TODO
	let compr = match args.compression {
		Compression::None => nrfs::Compression::None,
		Compression::Lz4 => nrfs::Compression::Lz4,
	};

	let keybuf;
	let (cipher, key_deriver) = if let Some(enc) = &args.encryption {
		let enc = match enc {
			Encryption::Chacha8Poly1305 => CipherType::ChaCha8Poly1305,
		};
		let kdf = match &args.key_derivation_function {
			KeyDerivationFunction::None => todo!("ask for file"),
			KeyDerivationFunction::Argon2id => {
				// TODO make m, t, p user configurable
				let m = 4096.try_into().unwrap();
				let t = (4 * 600).try_into().unwrap();
				let p = 4.try_into().unwrap();
				let pwd_a = rpassword::prompt_password("Enter new password: ")
					.expect("failed to ask password");
				let pwd_b = rpassword::prompt_password("Confirm password: ")
					.expect("failed to ask password");
				if pwd_a != pwd_b {
					eprintln!("Passwords do not match");
					std::process::exit(1);
				}
				keybuf = pwd_a.into_bytes();
				nrfs::KeyDeriver::Argon2id { password: &keybuf, m, t, p }
			}
		};
		(enc, kdf)
	} else {
		(
			nrfs::CipherType::NoneXxh3,
			nrfs::KeyDeriver::None { key: &[0; 32] },
		)
	};

	let s = FileDev::new(f, block_size);

	let config = nrfs::NewConfig {
		cipher,
		key_deriver,
		mirrors: vec![vec![s]],
		block_size,
		max_record_size: rec_size,
		dir: opt,
		compression: compr,
		cache_size: args.cache_size,
	};

	eprintln!("Creating filesystem");
	let nrfs = Nrfs::new(config).await.unwrap();

	let bg = nrfs::Background::default();

	bg.run(async {
		if let Some(d) = &args.directory {
			eprintln!("Adding files from {:?}", d);
			let root = nrfs.root_dir(&bg).await?;
			add_files(root, d, &args, extensions).await;
		}
		nrfs.finish_transaction(&bg).await
	})
	.await
	.unwrap();

	bg.drop().await.unwrap();
	dbg!(nrfs.statistics());
	nrfs.unmount().await.unwrap();

	async fn add_files(
		root: nrfs::DirRef<'_, '_, FileDev>,
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
				let mut u = nrfs::dir::ext::unix::Entry::new(0o700, 0, 0);
				let p = m.permissions();
				#[cfg(target_family = "unix")]
				{
					u.permissions = (p.mode() & 0o777) as _;
					u.set_uid(m.uid());
					u.set_gid(m.gid());
				}
				u
			});

			ext.mtime = extensions.mtime().then(|| nrfs::dir::ext::mtime::Entry {
				mtime: m
					.modified()
					.ok()
					.and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
					.map(|t| t.as_micros().try_into().unwrap_or(i64::MAX))
					.unwrap_or(0),
			});

			if m.is_file() || (m.is_symlink() && args.follow) {
				let c = fs::read(f.path()).unwrap();
				let f = root.create_file(n, &ext).await.unwrap().unwrap();
				f.write_grow(0, &c).await.unwrap();
				f.drop().await.unwrap();
			} else if m.is_dir() {
				// FIXME randomize key
				let opt = nrfs::DirOptions { extensions, ..nrfs::DirOptions::new(&[0; 16]) };
				let d = root.create_dir(n, &opt, &ext).await.unwrap().unwrap();
				let path = f.path();
				let fut: Pin<Box<dyn Future<Output = ()>>> =
					Box::pin(add_files(d, &path, args, extensions));
				fut.await;
			} else if m.is_symlink() {
				let c = fs::read_link(f.path()).unwrap();
				let f = root.create_sym(n, &ext).await.unwrap().unwrap();
				f.write_grow(0, c.to_str().unwrap().as_bytes())
					.await
					.unwrap();
				f.drop().await.unwrap();
			} else {
				todo!()
			}
		}
		root.drop().await.unwrap();
	}
}

async fn dump(args: Dump) {
	let retrieve_key = &mut |use_password| {
		if use_password {
			rpassword::prompt_password("Password: ")
				.expect("failed to ask password")
				.into_bytes()
		} else {
			todo!("ask for key file")
		}
	};

	let f = File::open(args.path).unwrap();
	// FIXME block size shouldn't matter.
	let s = FileDev::new(f, BlockSize::from_raw(12).unwrap());

	let conf = nrfs::LoadConfig {
		key_password: nrfs::KeyPassword::Key(&[0; 32]),
		retrieve_key,
		devices: vec![s],
		cache_size: args.cache_size,
		allow_repair: true,
	};
	let nrfs = Nrfs::load(conf).await.unwrap();

	let bg = nrfs::Background::default();

	bg.run(async {
		let root = nrfs.root_dir(&bg).await?;
		list_files(root, 0).await;
		Ok::<_, nrfs::Error<_>>(())
	})
	.await
	.unwrap();

	bg.drop().await.unwrap();

	async fn list_files(root: nrfs::DirRef<'_, '_, FileDev>, indent: usize) {
		let mut i = 0;
		while let Some((e, next_i)) = root.next_from(i).await.unwrap() {
			let data = e.data().await.unwrap();

			if let Some(u) = data.ext_unix {
				let mut s = [0; 9];
				for (i, (c, l)) in s.iter_mut().zip(b"rwxrwxrwx").rev().enumerate() {
					*c = [b'-', *l][usize::from(u.permissions & 1 << i != 0)];
				}
				print!(
					"{} {:>4} {:>4}  ",
					std::str::from_utf8(&s).unwrap(),
					u.uid(),
					u.gid(),
				);
			}

			if let Some(t) = data.ext_mtime {
				let secs = (t.mtime / 1_000_000) as i64;
				let micros = t.mtime.rem_euclid(1_000_000) as u32;
				let t = chrono::NaiveDateTime::from_timestamp_opt(secs, micros * 1_000).unwrap();
				// Use format!() since NaiveDateTime doesn't respect flags
				print!("{:<26}", format!("{}", t));
			}

			let name = e.key(&data).await.unwrap();
			let name = String::from_utf8_lossy(name.as_ref().map_or(b"", |n| n));

			match e {
				ItemRef::File(f) => {
					println!(
						"{:>8}  {:>indent$}{}f {}",
						f.len().await.unwrap(),
						"",
						[' ', 'e'][usize::from(f.is_embedded())],
						name,
						indent = indent
					);
					f.drop().await.unwrap();
				}
				ItemRef::Dir(d) => {
					println!(
						"{:>8}  {:>indent$} d {}",
						d.len().await.unwrap(),
						"",
						name,
						indent = indent
					);
					let fut: Pin<Box<dyn Future<Output = _>>> = Box::pin(list_files(d, indent + 2));
					fut.await;
				}
				ItemRef::Sym(f) => {
					let len = f.len().await.unwrap();
					let mut buf = vec![0; len as _];
					f.read_exact(0, &mut buf).await.unwrap();
					let link = String::from_utf8_lossy(&buf);
					println!(
						"{:>indent$}{}s {} -> {}",
						"",
						[' ', 'e'][usize::from(f.is_embedded())],
						name,
						link,
						indent = 10 + indent
					);
					f.drop().await.unwrap();
				}
				ItemRef::Unknown(e) => {
					println!("     ???  {:>indent$} ? {}", "", name, indent = indent);
					e.drop().await.unwrap();
				}
			}
			i = next_i
		}
		root.drop().await.unwrap();
	}
}
