//#![cfg_attr(not(test), no_std)]
#![deny(elided_lifetimes_in_paths)]

pub mod dir;
mod file;
mod name;
#[cfg(test)]
mod test;

pub use {
	dir::Dir,
	file::File,
	name::Name,
	nros::{Read, Storage, Write},
};

use core::fmt;

pub struct Nrfs<S: Storage> {
	storage: nros::Nros<S>,
}

impl<S: Storage> Nrfs<S> {
	pub fn new(storage: S, max_record_length_p2: u8) -> Result<Self, nros::NewError<S>> {
		let storage = nros::Nros::new(storage, max_record_length_p2)?;
		let mut s = Self { storage };
		match Dir::new(&mut s, [0; 16]) {
			Ok(_) => {}
			_ => todo!(),
		}
		Ok(s)
	}

	pub fn load(storage: S) -> Result<Self, nros::LoadError<S>> {
		Ok(Self { storage: nros::Nros::load(storage)? })
	}

	pub fn root_dir(&mut self) -> Result<Dir<'_, S>, Error<S>> {
		Dir::load(self, 0)
	}

	fn read(&mut self, id: u64, offset: u64, buf: &mut [u8]) -> Result<usize, Error<S>> {
		self.storage
			.read_object(id, offset, buf)
			.map_err(Error::Nros)
	}

	fn read_exact(&mut self, id: u64, offset: u64, buf: &mut [u8]) -> Result<(), Error<S>> {
		self.read(id, offset, buf)
			.and_then(|l| (l == buf.len()).then_some(()).ok_or(Error::Truncated))
	}

	fn write(&mut self, id: u64, offset: u64, data: &[u8]) -> Result<usize, Error<S>> {
		self.storage
			.write_object(id, offset, data)
			.map(|()| data.len())
			.map_err(Error::Nros)
	}

	fn write_all(&mut self, id: u64, offset: u64, data: &[u8]) -> Result<(), Error<S>> {
		self.storage
			.write_object(id, offset, data)
			.map_err(Error::Nros)
	}

	fn truncate(&mut self, id: u64, len: u64) -> Result<(), Error<S>> {
		self.storage.truncate_object(id, len).map_err(Error::Nros)
	}

	fn length(&mut self, id: u64) -> Result<u64, Error<S>> {
		self.storage.object_len(id).map_err(Error::Nros)
	}
}

pub enum Error<S>
where
	S: Storage,
{
	Nros(nros::Error<S>),
	Truncated,
	CorruptExtension,
	UnknownHashAlgorithm(u8),
}

impl<S> fmt::Debug for Error<S>
where
	S: Storage,
	S::Error: fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Nros(e) => f.debug_tuple("Nros").field(e).finish(),
			Self::Truncated => f.debug_tuple("Truncated").finish(),
			Self::CorruptExtension => f.debug_tuple("CorruptExtension").finish(),
			Self::UnknownHashAlgorithm(n) => {
				f.debug_tuple("UnknownHashAlgorithm").field(&n).finish()
			}
		}
	}
}
