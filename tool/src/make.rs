#[cfg(target_family = "unix")]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::{io::Read, os::unix::fs::FileTypeExt};

use {
	crate::{Compression, Encryption},
	nrfs::Item,
	std::{
		error::Error,
		fs::{self, Metadata, OpenOptions},
		future::Future,
		io::Write,
		num::{NonZeroU32, NonZeroU8},
		path::{Path, PathBuf},
		pin::Pin,
		str::FromStr,
	},
};

/// Default parameters.
///
/// Should be kept up-to-date with latest recommendations.
mod defaults {
	pub mod argon2id {
		use std::num::{NonZeroU32, NonZeroU8};

		pub const M: NonZeroU32 = NonZeroU32::new(1 << 20).unwrap(); // 1 GiB
		pub const T: NonZeroU32 = NonZeroU32::new(10).unwrap(); // 10 iterations
		pub const P: NonZeroU8 = NonZeroU8::new(1).unwrap(); // 1 thread

		pub const M_MIN: u64 = 1 << 15; // 32 MiB
		pub const T_MIN: u64 = 6; // 6 iterations
		pub const P_MIN: u64 = 1; // 1 thread
	}
}

/// Create a new filesystem.
#[derive(clap::Args)]
pub struct Make {
	/// The paths to the images to write the filesystem to.
	///
	/// To define a chain of images, specify paths with a comma inbetween them.
	///
	/// Examples:
	///
	/// * Single image: `a.img`
	///
	/// * Mirror (RAID1): `a.img b.img`
	///
	/// * Chain (RAID0): `a.img,b.img`.
	///
	/// * Mirror of chains (RAID10): `a.img,b.img c.img,d.img`
	///
	/// * Mirror of chains with mixed devices: `a.img,b.img c.img`
	#[clap(value_parser = parse_mirrors)]
	paths: Vec<Vec<Box<str>>>,
	/// The directory to copy to the image.
	#[clap(short, long)]
	directory: Option<PathBuf>,
	/// Whether to resolve symlinks when copying a directory.
	#[clap(short, long)]
	follow: bool,
	/// The record size to use.
	#[clap(short, long, value_parser = 9..=24, default_value_t = 17)]
	record_size_p2: i64,
	/// The block size to use.
	#[clap(short, long, value_parser = 9..=24, default_value_t = 12)]
	block_size_p2: i64,
	/// The compression to use.
	#[clap(short, long, value_enum, default_value = "lz4")]
	compression: Compression,
	/// Encryption to use on the filesystem.
	#[clap(short, long, value_enum)]
	encryption: Option<Encryption>,
	/// Which algorithm to use to derive the key for encryption.
	///
	/// If none, a 32-byte key must be supplied.
	///
	/// Possible values: none, argon2id[,m,t,p].
	#[arg(short, long, default_value = "argon2id")]
	key_derivation_function: KeyDerivationFunction,
	/// Soft limit on the cache size.
	#[clap(long, default_value_t = 1 << 27)]
	cache_size: usize,
	/// File to load or save the key to.
	///
	/// If a key derivation function is specified, the generated key will be saved to this file.
	#[arg(short = 'K', long)]
	key_file: Option<String>,
}

#[derive(Clone)]
enum KeyDerivationFunction {
	None,
	Argon2id { m: NonZeroU32, t: NonZeroU32, p: NonZeroU8 },
}

impl FromStr for KeyDerivationFunction {
	type Err = &'static str;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		use defaults::argon2id::*;
		Ok(match s {
			"none" => KeyDerivationFunction::None,
			"argon2id" => {
				let p = u8::try_from(num_cpus::get()).unwrap_or(0);
				Self::Argon2id { m: M, t: T, p: NonZeroU8::new(p).unwrap_or(P) }
			}
			_ if s.starts_with("argon2id,") => {
				let mut it = s.split(',').skip(1);
				let m = it.next().ok_or("expected 'm' argument")?;
				let t = it.next().ok_or("expected 't' argument")?;
				let p = it.next().ok_or("expected 'p' argument")?;
				let m = m
					.parse::<u64>()
					.map_err(|_| "expected integer value for 'm'")?;
				let t = t
					.parse::<u64>()
					.map_err(|_| "expected integer value for 't'")?;
				let p = p
					.parse::<u64>()
					.map_err(|_| "expected integer value for 'p'")?;
				(M_MIN..1 << 28)
					.contains(&m)
					.then(|| ())
					.ok_or("'m' value out of range")?;
				(T_MIN..1 << 24)
					.contains(&t)
					.then(|| ())
					.ok_or("'t' value out of range")?;
				(P_MIN..256)
					.contains(&p)
					.then(|| ())
					.ok_or("'p' value out of range")?;
				let m = NonZeroU32::new(m.try_into().unwrap()).unwrap();
				let t = NonZeroU32::new(t.try_into().unwrap()).unwrap();
				let p = NonZeroU8::new(p.try_into().unwrap()).unwrap();
				Self::Argon2id { m, t, p }
			}
			_ => return Err("unknown KDF algorithm"),
		})
	}
}

fn parse_mirrors(s: &str) -> Result<Vec<Box<str>>, &'static str> {
	Ok(s.split(',').map(From::from).collect())
}

pub async fn make(args: Make) -> Result<(), Box<dyn Error>> {
	let block_size = nrfs::BlockSize::from_raw(args.block_size_p2.try_into().unwrap()).unwrap();
	let max_record_size =
		nrfs::MaxRecordSize::from_raw(args.record_size_p2.try_into().unwrap()).unwrap();

	let mirrors = args
		.paths
		.into_iter()
		.map(|chain| {
			chain
				.into_iter()
				.map(|path| {
					OpenOptions::new()
						.truncate(false)
						.read(true)
						.write(true)
						.open(&*path)
						.map(nrfs::dev::FileDev::new)
				})
				.try_collect()
		})
		.try_collect()?;

	// FIXME randomize key

	let keybuf;
	let (cipher, key_deriver) = if let Some(enc) = args.encryption {
		let enc = match enc {
			Encryption::XChacha12Poly1305 => nrfs::CipherType::XChaCha12Poly1305,
		};
		let kdf = match args.key_derivation_function {
			KeyDerivationFunction::None => todo!("ask for file"),
			KeyDerivationFunction::Argon2id { m, t, p } => {
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

	let config = nrfs::NewConfig {
		cipher,
		key_deriver,
		mirrors,
		block_size,
		max_record_size,
		compression: args.compression.into(),
		cache_size: args.cache_size,
	};

	eprintln!("Creating filesystem");
	let nrfs = nrfs::Nrfs::new(config).await?;

	nrfs.run(async {
		let buf = &mut vec![0; 1 << max_record_size.to_raw()];
		if let Some(d) = &args.directory {
			// TODO attrs on root dir
			eprintln!("Adding files from {:?}", d);
			add_files(nrfs.root_dir(), d, args.follow, buf).await?;
		} else {
			// TODO attrs on root dir
		}

		nrfs.finish_transaction().await?;
		Ok::<_, Box<dyn Error>>(())
	})
	.await?;

	// Get key and unmount now so the user doesn't have to start all over again
	// in case an error occurs later.
	let key = nrfs.header_key();
	nrfs.unmount().await?;

	if let Some(key_file) = args.key_file {
		eprintln!("Saving key to {:?}", key_file);
		let mut opt = fs::OpenOptions::new();
		opt.create(true);
		opt.write(true);
		#[cfg(unix)]
		opt.mode(0o400); // read-only
		opt.open(key_file)?.write_all(&key)?;
	}

	Ok(())
}

async fn add_files(
	root: nrfs::Dir<'_, nrfs::dev::FileDev>,
	from: &Path,
	follow_symlinks: bool,
	buf: &mut [u8],
) -> Result<(), Box<dyn Error>> {
	for f in fs::read_dir(from).expect("failed to read dir") {
		let f = f?;
		let m = f.metadata()?;
		let n = f.file_name();
		let n = n.to_str().unwrap().try_into().unwrap();

		if m.is_file() || (m.is_symlink() && follow_symlinks) {
			let mut inp = fs::File::open(f.path())?;
			let f = root.create_file(n).await?.unwrap();
			setattr(&f, &m).await?;
			let mut offt = 0;
			loop {
				let l = inp.read(buf)?;
				if l == 0 {
					break;
				}
				f.write_grow(offt, &buf[..l]).await??;
				offt += u64::try_from(l).unwrap();
			}
		} else if m.is_dir() {
			let d = root.create_dir(n).await?.unwrap();
			setattr(&d, &m).await?;
			let path = f.path();
			let fut: Pin<Box<dyn Future<Output = _>>> =
				Box::pin(add_files(d, &path, follow_symlinks, buf));
			fut.await?;
		} else if m.is_symlink() {
			let c = fs::read_link(f.path())?;
			let f = root.create_sym(n).await?.unwrap();
			setattr(&f, &m).await?;
			f.write_grow(0, c.to_str().unwrap().as_bytes()).await??;
		} else {
			todo!()
		}
	}
	Ok(())
}

async fn setattr(
	item: &Item<'_, nrfs::dev::FileDev>,
	meta: &Metadata,
) -> Result<(), Box<dyn Error>> {
	let mtime = || {
		let Ok(t) = meta.modified() else { return 0 };
		match t.duration_since(std::time::UNIX_EPOCH) {
			Ok(t) => t.as_micros().try_into().unwrap_or(i64::MAX),
			Err(e) => -(e.duration().as_micros().try_into().unwrap_or(i64::MAX)),
		}
	};
	item.set_modified_time(mtime()).await?;
	if cfg!(target_family = "unix") {
		item.set_attr(b"nrfs.uid".into(), encode_u(&meta.uid().to_le_bytes()))
			.await??;
		item.set_attr(b"nrfs.gid".into(), encode_u(&meta.gid().to_le_bytes()))
			.await??;
		let mode = meta.permissions().mode();
		let mode = (mode & 0o777) as u16;
		let mode = mode
			| if meta.file_type().is_fifo() {
				crate::unix::TY_PIPE
			} else if meta.file_type().is_socket() {
				crate::unix::TY_SOCK
			} else if meta.file_type().is_block_device() {
				crate::unix::TY_BLOCK
			} else if meta.file_type().is_char_device() {
				crate::unix::TY_CHAR
			} else {
				// Assume file, dir or symlink
				crate::unix::TY_BUILTIN
			};
		item.set_attr(b"nrfs.unixmode".into(), &mode.to_le_bytes())
			.await??;
	}
	Ok(())
}

fn encode_u(mut b: &[u8]) -> &[u8] {
	while let Some((&0, c)) = b.split_last() {
		b = c;
	}
	b
}
