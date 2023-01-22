#![forbid(unused_must_use)]
#![forbid(rust_2018_idioms)]

mod fs;
mod job;

use fuser::MountOption;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	env_logger::init();

	let mut a = std::env::args().skip(1);
	let f = a.next().ok_or("expected file path")?;
	let m = a.next().ok_or("expected mount path")?;

	let f = std::fs::OpenOptions::new()
		.read(true)
		.write(true)
		.open(&f)?;
	let (f, channel) = futures_executor::block_on(fs::Fs::new(f));
	let session = fuser::spawn_mount2(
		channel,
		m,
		&[
			MountOption::FSName("nrfs".into()),
			MountOption::DefaultPermissions,
		],
	)?;

	futures_executor::block_on(f.run()).unwrap();

	session.join();

	Ok(())
}
