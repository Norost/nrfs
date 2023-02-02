use std::{
	error::Error,
	fs::{self, File},
	future::Future,
	pin::Pin,
};

/// Dump the contents of a filesystem.
#[derive(clap::Args)]
pub struct Dump {
	/// Paths to the filesystem's images.
	paths: Vec<String>,
	/// Soft limit on the global cache size.
	#[clap(long, default_value_t = 1 << 27)]
	cache_size: usize,
	/// File containing the key to decrypt the filesystem header with.
	#[arg(short = 'K', long)]
	key_file: Option<String>,
}

/// Additional statistics collected while iterating.
#[derive(Debug, Default)]
struct Statistics {
	/// Total amount of unembedded files.
	files: u64,
	/// Total amount of unembedded symlinks.
	symlinks: u64,
	/// Total amount of directories.
	directories: u64,
	/// Total amount of embedded files.
	embedded_files: u64,
	/// Total amount of embedded symlinks.
	embedded_symlinks: u64,
	/// Total amount of unknown entry types.
	unknown: u64,
	/// Total amount of dangling entries.
	dangling: u64,
}

pub async fn dump(args: Dump) -> Result<(), Box<dyn Error>> {
	let key = args
		.key_file
		.map(|key_file| {
			eprintln!("Loading key from {:?}", &key_file);
			let key = fs::read(&key_file)?;
			let key: [u8; 32] = key.try_into().map_err(|_| "key is not 32 bytes long")?;
			Ok::<_, Box<dyn Error>>(key)
		})
		.transpose()?;

	let retrieve_key = &mut |use_password| {
		if let Some(key) = key {
			Some(nrfs::KeyPassword::Key(key))
		} else if use_password {
			let pwd = rpassword::prompt_password("Password: ").expect("failed to ask password");
			Some(nrfs::KeyPassword::Password(pwd.into_bytes()))
		} else {
			None
		}
	};

	let devices = args
		.paths
		.into_iter()
		.map(|p| {
			File::open(p).map(|f| {
				// FIXME block size shouldn't matter.
				nrfs::dev::FileDev::new(f, nrfs::BlockSize::from_raw(12).unwrap())
			})
		})
		.try_collect()?;

	let conf = nrfs::LoadConfig {
		retrieve_key,
		devices,
		cache_size: args.cache_size,
		allow_repair: false,
	};
	let nrfs = nrfs::Nrfs::load(conf).await?;

	let bg = nrfs::Background::default();

	let mut stat = Statistics::default();

	bg.run(async {
		let root = nrfs.root_dir(&bg).await?;
		list_files(root, &mut stat, 0).await?;
		Ok::<_, Box<dyn Error>>(())
	})
	.await?;

	bg.drop().await?;

	println!();

	let e = |name: &str, val: &dyn std::fmt::Display| {
		println!("{}: {:>indent$}", name, val, indent = 34 - name.len())
	};

	e("files", &stat.files);
	e("symlinks", &stat.symlinks);
	e("directories", &stat.directories);
	e("directories", &stat.directories);
	e("embedded files", &stat.embedded_files);
	e("embedded symlinks", &stat.embedded_symlinks);
	e("unknown", &stat.unknown);
	e("dangling", &stat.dangling);
	println!();

	let stat = nrfs.statistics();
	let obj = &stat.object_store;
	let sto = &obj.storage;
	let alloc = &sto.allocation;
	e("block size", &format!("2**{}", sto.block_size.to_raw()));
	e("used objects", &obj.used_objects);
	e("used blocks", &alloc.used_blocks);

	Ok(())
}

async fn list_files(
	root: nrfs::DirRef<'_, '_, nrfs::dev::FileDev>,
	stats: &mut Statistics,
	indent: usize,
) -> Result<(), Box<dyn Error>> {
	let mut i = 0;
	while let Some((e, next_i)) = root.next_from(i).await? {
		let data = e.data().await?;

		if let Some(u) = data.ext_unix {
			let mut s = [0; 9];
			for (i, (c, l)) in s.iter_mut().zip(b"rwxrwxrwx").rev().enumerate() {
				*c = [b'-', *l][usize::from(u.permissions & 1 << i != 0)];
			}
			print!(
				"{} {:>4} {:>4}  ",
				std::str::from_utf8(&s)?,
				u.uid(),
				u.gid(),
			);
		}

		if let Some(t) = data.ext_mtime {
			let secs = (t.mtime / 1_000_000) as i64;
			let micros = t.mtime.rem_euclid(1_000_000) as u32;
			let t = chrono::NaiveDateTime::from_timestamp_opt(secs, micros * 1_000).unwrap();
			// Use format!() since NaiveDateTime doesn't respect flags
			print!("{:<26} ", format!("{}", t));
		}

		let name = e.key(&data).await?;
		let name = match &name {
			Some(name) => String::from_utf8_lossy(name.as_ref()),
			None => {
				stats.dangling += 1;
				"".into()
			}
		};

		use nrfs::dir::ItemRef;
		match e {
			ItemRef::File(f) => {
				if f.is_embedded() {
					stats.embedded_files += 1;
				} else {
					stats.files += 1;
				}
				println!(
					"{:>8}  {:>indent$}{}f {}",
					f.len().await?,
					"",
					[' ', 'e'][usize::from(f.is_embedded())],
					name,
					indent = indent + 4 + 8
				);
				f.drop().await?;
			}
			ItemRef::Dir(d) => {
				stats.directories += 1;
				println!(
					"{:>8} heap:{:<8}  {:>indent$} d {}",
					format!("{}/{}", d.len().await?, d.capacity().await?),
					d.heap_size().await?,
					"",
					name,
					indent = indent
				);
				let fut: Pin<Box<dyn Future<Output = _>>> =
					Box::pin(list_files(d, stats, indent + 2));
				fut.await?;
			}
			ItemRef::Sym(f) => {
				if f.is_embedded() {
					stats.embedded_symlinks += 1;
				} else {
					stats.symlinks += 1;
				}
				let len = f.len().await?;
				let mut buf = vec![0; len as _];
				f.read_exact(0, &mut buf).await?;
				let link = String::from_utf8_lossy(&buf);
				println!(
					"{:>indent$}{}s {} -> {}",
					"",
					[' ', 'e'][usize::from(f.is_embedded())],
					name,
					link,
					indent = 10 + indent + 4 + 8
				);
				f.drop().await?;
			}
			ItemRef::Unknown(e) => {
				stats.unknown += 1;
				println!("     ???  {:>indent$} ? {}", "", name, indent = indent);
				e.drop().await?;
			}
		}
		i = next_i
	}
	root.drop().await?;

	Ok(())
}
