#![forbid(unused_must_use)]
#![forbid(rust_2018_idioms)]
#![feature(pin_macro)]

mod dump;
mod make;

#[cfg(target_family = "unix")]
use clap::Parser;

#[derive(Debug, clap::Parser)]
#[clap(
	author = "David Hoppenbrouwers",
	version = "0.2",
	about = "Tool for creating & working with NRFS filesystems"
)]
enum Command {
	Make(make::Make),
	Dump(dump::Dump),
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum Compression {
	None,
	Lz4,
}

impl From<Compression> for nrfs::Compression {
	fn from(compression: Compression) -> Self {
		match compression {
			Compression::None => nrfs::Compression::None,
			Compression::Lz4 => nrfs::Compression::Lz4,
		}
	}
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum Encryption {
	Chacha8Poly1305,
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum KeyDerivationFunction {
	None,
	Argon2id,
}

fn main() {
	let fut = async {
		match Command::parse() {
			Command::Make(args) => make::make(args).await,
			Command::Dump(args) => dump::dump(args).await,
		}
	};
	futures_executor::block_on(fut);
}
