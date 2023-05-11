#![forbid(unused_must_use, rust_2018_idioms)]
#![feature(const_option, iterator_try_collect)]

mod dump;
mod extract_key;
mod make;

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
