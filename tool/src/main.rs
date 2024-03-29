#![deny(unused_must_use, rust_2018_idioms)]
#![feature(const_option, iterator_try_collect)]

mod dump;
mod extract_key;
mod make;
mod resize;

use clap::{builder::PossibleValue, Parser};

#[derive(clap::Parser)]
#[clap(
	author = "David Hoppenbrouwers",
	version = "0.3",
	about = "Tool for creating & working with NRFS filesystems"
)]
enum Command {
	Make(make::Make),
	ExtractKey(extract_key::ExtractKey),
	Dump(dump::Dump),
	Resize(resize::Resize),
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
	XChacha12Poly1305,
}

impl clap::ValueEnum for Encryption {
	fn value_variants<'a>() -> &'a [Self] {
		&[Self::XChacha12Poly1305]
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		Some(match self {
			Self::XChacha12Poly1305 => PossibleValue::new("xchacha12poly1305"),
		})
	}
}

#[cfg(unix)]
mod unix {
	pub const TY_BUILTIN: u16 = 0 << 9;
	pub const TY_BLOCK: u16 = 1 << 9;
	pub const TY_CHAR: u16 = 2 << 9;
	pub const TY_PIPE: u16 = 3 << 9;
	pub const TY_SOCK: u16 = 4 << 9;
	#[allow(dead_code)]
	pub const TY_DOOR: u16 = 5 << 9;
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let fut = async {
		match Command::parse() {
			Command::Make(args) => make::make(args).await,
			Command::ExtractKey(args) => extract_key::extract_key(args).await,
			Command::Dump(args) => dump::dump(args).await,
			Command::Resize(args) => resize::resize(args).await,
		}
	};
	futures_executor::block_on(fut)
}
