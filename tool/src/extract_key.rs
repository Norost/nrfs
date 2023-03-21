#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
	error::Error,
	fs::{File, OpenOptions},
	io::Write,
};

/// Extract the key necessary to decrypt the filesystem header.
#[derive(clap::Args)]
pub struct ExtractKey {
	/// File to which to save the header key.
	key_file: String,
	/// Paths to the filesystem's images.
	paths: Vec<String>,
}

pub async fn extract_key(args: ExtractKey) -> Result<(), Box<dyn Error>> {
	let retrieve_key = &mut |use_password| {
		if use_password {
			let pwd = rpassword::prompt_password("Password: ").expect("failed to ask password");
			Some(nros::KeyPassword::Password(pwd.into_bytes()))
		} else {
			None
		}
	};

	let devices = args
		.paths
		.into_iter()
		.map(|p| File::open(p).map(nros::dev::FileDev::new))
		.try_collect()?;

	let conf = nros::LoadConfig {
		retrieve_key,
		devices,
		cache_size: 0,
		allow_repair: false,
		magic: *b"NRFS",
		resource: nros::StdResource::new(),
	};

	// Use nros to avoid fetching any records.
	let nros = nros::Nros::load(conf).await?;
	let key = nros.header_key();

	eprintln!("Saving key to {:?}", &args.key_file);
	let mut opt = OpenOptions::new();
	opt.create(true);
	opt.write(true);
	#[cfg(unix)]
	opt.mode(0o400); // read-only
	opt.open(args.key_file)?.write_all(&key)?;

	Ok(())
}
