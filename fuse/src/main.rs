#![forbid(unused_must_use)]
#![forbid(rust_2018_idioms)]
#![feature(iterator_try_collect)]

mod fs;
mod job;

use fuser::MountOption;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	env_logger::init();

	let mut a = std::env::args().skip(1);
	let m = a.next().ok_or("expected mount path")?;

	let f = a
		.map(|path| {
			std::fs::OpenOptions::new()
				.read(true)
				.write(true)
				.open(&path)
		})
		.try_collect::<Vec<_>>()?;

	let (f, channel) = futures_executor::block_on(fs::Fs::new(0o755, f.into_iter()));

	let mut opts = vec![
		MountOption::FSName("nrfs".into()),
		MountOption::DefaultPermissions,
	];

	if unsafe { libc::getuid() } == 0 {
		eprintln!("Enabling allow_other");
		opts.extend_from_slice(&[MountOption::AllowOther]);
	}

	let session = fuser::spawn_mount2(channel, m, &opts)?;

	futures_executor::block_on(f.run()).unwrap();

	session.join();

	Ok(())
}
