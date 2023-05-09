#![forbid(unused_must_use)]
#![forbid(rust_2018_idioms)]
#![feature(iterator_try_collect)]

mod dev;
mod fs;
mod job;

use {clap::Parser, fuser::MountOption, std::error::Error};

#[cfg(feature = "dhat")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// FUSE driver for NRFS.
#[derive(Parser)]
struct Args {
	/// Path to mount filesystem on.
	mount: String,
	/// Paths to filesystem images.
	images: Vec<String>,
	/// File to load key from.
	#[arg(short = 'K', long)]
	key_file: Option<String>,
	/// Soft limit on the cache size.
	#[arg(long, default_value_t = 1 << 27)]
	cache_size: usize,
	#[arg(long, default_value_t = 15)]
	sync_interval: u32,
}

fn main() -> Result<(), Box<dyn Error>> {
	#[cfg(feature = "dhat")]
	let _profiler = dhat::Profiler::new_heap();

	env_logger::init();

	let args = Args::parse();

	let key = args
		.key_file
		.map(|key_file| {
			let key = std::fs::read(key_file)?;
			let key: [u8; 32] = key.try_into().map_err(|_| "key is not 32 bytes long")?;
			Ok::<_, Box<dyn Error>>(key)
		})
		.transpose()?;

	let f = args
		.images
		.into_iter()
		.map(|path| {
			std::fs::OpenOptions::new()
				.read(true)
				.write(true)
				.open(&path)
		})
		.try_collect::<Vec<_>>()?;

	let (f, channel) =
		futures_executor::block_on(fs::Fs::new(0o755, f.into_iter(), key, args.cache_size));
	let mut sync_channel = channel.clone();

	let mut opts = vec![
		MountOption::FSName("nrfs".into()),
		MountOption::DefaultPermissions,
	];

	if unsafe { libc::getuid() } == 0 {
		eprintln!("Enabling allow_other");
		opts.extend_from_slice(&[MountOption::AllowOther]);
	}

	let session = fuser::spawn_mount2(channel, args.mount, &opts)?;

	let sync_interval = args.sync_interval;
	std::thread::spawn(move || loop {
		std::thread::sleep(std::time::Duration::from_secs(sync_interval.into()));
		sync_channel.commit();
	});

	futures_executor::block_on(f.run()).unwrap();

	session.join();

	Ok(())
}
