#[cfg(target_family = "unix")]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use {
	crate::{Compression, Encryption, KeyDerivationFunction},
	std::{
		fs::{self, OpenOptions},
		future::Future,
		path::{Path, PathBuf},
		pin::Pin,
	},
};

/// Create a new filesystem.
#[derive(Debug, clap::Args)]
pub struct Make {
	/// The path to the image to write the filesystem to.
	path: String,
	/// The directory to copy to the image.
	#[clap(short, long)]
	directory: Option<PathBuf>,
	/// Whether to resolve symlinks when copying a directory.
	#[clap(short, long)]
	follow: bool,
	/// The record size to use.
	#[clap(short, long, value_parser = 9..=24, default_value_t = 17)]
	record_size_p2: u8,
	/// The block size to use.
	#[clap(short, long, value_parser = 9..=24, default_value_t = 12)]
	block_size_p2: u8,
	/// The compression to use.
	#[clap(short, long, value_enum, default_value = "lz4")]
	compression: Compression,
	/// Encryption to use on the filesystem.
	#[clap(short, long, value_enum)]
	encryption: Option<Encryption>,
	/// Which algorithm to use to derive the key for encryption.
	///
	/// If none, a 32-byte key must be supplied.
	#[clap(short, long, value_enum, default_value = "argon2id")]
	key_derivation_function: KeyDerivationFunction,
	/// Soft limit on the cache size.
	#[clap(long, default_value_t = 1 << 27)]
	cache_size: usize,
}

pub async fn make(args: Make) {
	let f = OpenOptions::new()
		.truncate(false)
		.read(true)
		.write(true)
		.open(&args.path)
		.unwrap();

	let block_size = nrfs::BlockSize::from_raw(args.block_size_p2).unwrap();

	let mut extensions = nrfs::dir::EnableExtensions::default();
	extensions.add_unix();
	extensions.add_mtime();
	// FIXME randomize key
	let opt = nrfs::DirOptions { extensions, ..nrfs::DirOptions::new(&[0; 16]) };
	let rec_size = nrfs::MaxRecordSize::K128; // TODO

	let keybuf;
	let (cipher, key_deriver) = if let Some(enc) = &args.encryption {
		let enc = match enc {
			Encryption::Chacha8Poly1305 => nrfs::CipherType::ChaCha8Poly1305,
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
		(nrfs::CipherType::NoneXxh3, nrfs::KeyDeriver::None { key: &[0; 32] })
	};

	let s = nrfs::dev::FileDev::new(f, block_size);

	let config = nrfs::NewConfig {
		cipher,
		key_deriver,
		mirrors: vec![vec![s]],
		block_size,
		max_record_size: rec_size,
		dir: opt,
		compression: args.compression.into(),
		cache_size: args.cache_size,
	};

	eprintln!("Creating filesystem");
	let nrfs = nrfs::Nrfs::new(config).await.unwrap();

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
	nrfs.unmount().await.unwrap();

	async fn add_files(
		root: nrfs::DirRef<'_, '_, nrfs::dev::FileDev>,
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
