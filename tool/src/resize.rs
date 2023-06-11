use {
	nrfs::dev::FileDev,
	std::{
		error::Error,
		fs::{self, OpenOptions},
	},
};

#[derive(clap::Args)]
pub struct Resize {
	blocks: u64,
	/// Paths to the filesystem's images.
	paths: Vec<String>,
	/// File containing the key to decrypt the filesystem header with.
	#[arg(short = 'K', long)]
	key_file: Option<String>,
}

pub async fn resize(args: Resize) -> Result<(), Box<dyn Error>> {
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
			OpenOptions::new()
				.create(false)
				.truncate(false)
				.read(true)
				.write(true)
				.open(&*p)
				.map(FileDev::new)
		})
		.try_collect()?;

	let conf = nrfs::LoadConfig { retrieve_key, devices, cache_size: 0, allow_repair: false };
	let fs = nrfs::Nrfs::load(conf).await?;

	fs.set_block_count(args.blocks).await?;
	fs.finish_transaction().await?;
	Ok(())
}
