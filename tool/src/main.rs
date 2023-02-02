#![forbid(unused_must_use, rust_2018_idioms)]
#![feature(const_option, iterator_try_collect, pin_macro)]

mod dump;
mod extract_key;
mod make;

#[cfg(target_family = "unix")]
use clap::Parser;

#[derive(clap::Parser)]
#[clap(
	author = "David Hoppenbrouwers",
	version = "0.2",
	about = "Tool for creating & working with NRFS filesystems"
)]
enum Command {
	Make(make::Make),
	ExtractKey(extract_key::ExtractKey),
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

#[derive(Clone, Debug)]
enum Encryption {
	Chacha8Poly1305,
}

impl std::str::FromStr for Encryption {
	type Err = &'static str;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Ok(match s {
			"chacha8poly1305" => Self::Chacha8Poly1305,
			_ => return Err("unknown cipher algorithm"),
		})
	}
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let fut = async {
		match Command::parse() {
			Command::Make(args) => make::make(args).await,
			Command::ExtractKey(args) => extract_key::extract_key(args).await,
			Command::Dump(args) => dump::dump(args).await,
		}
	};
	futures_executor::block_on(fut)
}
