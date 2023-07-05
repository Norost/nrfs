use crate::unix;

use {
	nrfs::{ItemTy, Nrfs},
	std::{
		error::Error,
		fs::{self, File},
		future::Future,
		pin::Pin,
	},
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
		.map(|p| File::open(p).map(nrfs::dev::FileDev::new))
		.try_collect()?;

	let conf = nrfs::LoadConfig {
		retrieve_key,
		devices,
		cache_size: args.cache_size,
		allow_repair: false,
	};
	let nrfs = nrfs::Nrfs::load(conf).await?;

	let mut stat = Statistics::default();

	nrfs.run(async {
		list_files(&nrfs, nrfs.root_dir(), &mut stat, 0).await?;
		Ok::<_, Box<dyn Error>>(())
	})
	.await?;

	println!();

	let e = |name: &str, val: &dyn std::fmt::Display| {
		println!("{}: {:>indent$}", name, val, indent = 34 - name.len())
	};

	e("directories", &stat.directories);
	e("files", &stat.files);
	e("symlinks", &stat.symlinks);
	e("embedded files", &stat.embedded_files);
	e("embedded symlinks", &stat.embedded_symlinks);
	e("unknown", &stat.unknown);
	println!();

	let stat = nrfs.statistics();
	let obj = &stat.object_store;
	let sto = &obj.storage;
	let alloc = &sto.allocation;
	e("block size", &format!("2**{}", sto.block_size.to_raw()));
	e(
		"max record size",
		&format!("2**{}", sto.max_record_size.to_raw()),
	);
	e("compression", &sto.compression);
	e("used objects", &obj.used_objects);
	e("used blocks", &alloc.used_blocks);
	e("total blocks", &alloc.total_blocks);

	Ok(())
}

async fn list_files(
	fs: &Nrfs<nrfs::dev::FileDev>,
	root: nrfs::Dir<'_, nrfs::dev::FileDev>,
	stats: &mut Statistics,
	indent: usize,
) -> Result<(), Box<dyn Error>> {
	let mut i = 0;
	while let Some((data, next_i)) = root.next_from(i).await? {
		let item = fs.item(data.key);

		let m = item.modified().await?;
		let fmt_time = |t: i64| {
			let secs = t / 1_000_000;
			let micros = t.rem_euclid(1_000_000) as u32;
			let t = chrono::NaiveDateTime::from_timestamp_opt(secs, micros * 1_000).unwrap();
			format!("{}", t)
		};
		print!("[{:<26},{:}] ", fmt_time(m.time), fmt_time(m.gen));

		let mut first = true;
		for k in item.attr_keys().await? {
			(!first).then(|| print!("|"));
			first = false;
			print!("{}:", k);
			let v = item.attr(&k).await?.unwrap();
			match &**k {
				b"nrfs.uid" | b"nrfs.gid" => print!("{}", decode_u(&v)),
				b"nrfs.unixmode" => {
					let u = decode_u(&v);
					let ty = match (u & 0o7_000) as u16 {
						unix::TY_BUILTIN => '-',
						unix::TY_CHAR => 'c',
						unix::TY_BLOCK => 'b',
						unix::TY_PIPE => 'p',
						unix::TY_SOCK => 's',
						unix::TY_DOOR => 'D',
						_ => '?',
					};
					let mut s = [0; 9];
					for (i, (c, l)) in s.iter_mut().zip(b"rwxrwxrwx").rev().enumerate() {
						*c = [b'-', *l][usize::from(u & 1 << i != 0)];
					}
					print!("{}{}", ty, std::str::from_utf8(&s).unwrap());
				}
				_ => print!("{:?}", bstr::BStr::new(&v)),
			}
		}

		let name = bstr::BStr::new(&**data.name);

		match data.ty {
			ItemTy::Dir => {
				let d = fs.dir(data.key).await?;
				stats.directories += 1;
				println!(
					"{:>12}  {:>indent$}  d {}",
					d.len().await?,
					"",
					name,
					indent = indent + 4 + 8
				);
				let fut: Pin<Box<dyn Future<Output = _>>> =
					Box::pin(list_files(fs, d, stats, indent + 2));
				fut.await?;
			}
			ItemTy::File | ItemTy::EmbedFile => {
				let f = fs.file(data.key);
				let is_embed = matches!(data.ty, ItemTy::EmbedFile);
				if is_embed {
					stats.embedded_files += 1;
				} else {
					stats.files += 1;
				}
				println!(
					"{:>12}  {:>indent$} {}f {}",
					f.len().await?,
					"",
					[' ', 'e'][usize::from(is_embed)],
					name,
					indent = indent + 4 + 8
				);
			}
			ItemTy::Sym | ItemTy::EmbedSym => {
				let f = fs.file(data.key);
				let is_embed = matches!(data.ty, ItemTy::EmbedSym);
				if is_embed {
					stats.embedded_symlinks += 1;
				} else {
					stats.symlinks += 1;
				}
				let len = f.len().await?;
				let (len, trim_len) = if len > 64 { (61, true) } else { (len, false) };
				let mut buf = vec![0; len as _];
				f.read(0, &mut buf).await?;
				let mut link = String::from_utf8_lossy(&buf);
				trim_len.then(|| link += "...");
				println!(
					"{:>12}  {:>indent$} {}s {} -> {}",
					"",
					"",
					[' ', 'e'][usize::from(is_embed)],
					name,
					link,
					indent = indent + 4 + 8
				);
			}
		}
		i = next_i
	}

	Ok(())
}

fn decode_u(b: &[u8]) -> u128 {
	let mut c = [0; 16];
	c[..b.len()].copy_from_slice(b);
	u128::from_le_bytes(c)
}
