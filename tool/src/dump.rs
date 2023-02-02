use {
	std::{fs::File, future::Future, pin::Pin},
};

/// Dump the contents of a filesystem.
#[derive(Debug, clap::Args)]
pub struct Dump {
	/// The path to the filesystem image.
	path: String,
	/// Soft limit on the global cache size.
	#[clap(long, default_value_t = 1 << 27)]
	cache_size: usize,
}

pub async fn dump(args: Dump) {
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
	let s = nrfs::dev::FileDev::new(f, nrfs::BlockSize::from_raw(12).unwrap());

	let conf = nrfs::LoadConfig {
		key_password: nrfs::KeyPassword::Key(&[0; 32]),
		retrieve_key,
		devices: vec![s],
		cache_size: args.cache_size,
		allow_repair: true,
	};
	let nrfs = nrfs::Nrfs::load(conf).await.unwrap();

	let bg = nrfs::Background::default();

	bg.run(async {
		let root = nrfs.root_dir(&bg).await?;
		list_files(root, 0).await;
		Ok::<_, nrfs::Error<_>>(())
	})
	.await
	.unwrap();

	bg.drop().await.unwrap();

	async fn list_files(root: nrfs::DirRef<'_, '_, nrfs::dev::FileDev>, indent: usize) {
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

			use nrfs::dir::ItemRef;
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
