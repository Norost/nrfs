//#![cfg_attr(not(test), no_std)]
#![deny(elided_lifetimes_in_paths)]
#![feature(pin_macro)]

pub mod dir;
mod file;
mod name;
#[cfg(test)]
mod test;

pub use {
	dir::{Dir, DirOptions},
	file::File,
	name::Name,
	nros::{BlockSize, Compression, Dev, MaxRecordSize},
};

use core::fmt;

#[derive(Debug)]
pub struct Nrfs<D: Dev> {
	storage: nros::Nros<D>,
}

impl<D: Dev> Nrfs<D> {
	pub async fn new<M, C>(
		mirrors: M,
		block_size: BlockSize,
		max_record_size: MaxRecordSize,
		dir: &DirOptions,
		compression: Compression,
		global_cache_size: usize,
		dirty_cache_size: usize,
	) -> Result<Self, Error<D>>
	where
		M: IntoIterator<Item = C>,
		C: IntoIterator<Item = D>,
	{
		let storage = nros::Nros::new(
			mirrors,
			block_size,
			max_record_size,
			compression,
			global_cache_size,
			dirty_cache_size,
		)
		.await?;
		let mut s = Self { storage };
		Dir::new(&mut s, dir).await?;
		Ok(s)
	}

	pub async fn load(
		devices: Vec<D>,
		global_cache_size: usize,
		dirty_cache_size: usize,
	) -> Result<Self, Error<D>> {
		Ok(Self {
			storage: nros::Nros::load(devices, global_cache_size, dirty_cache_size).await?,
		})
	}

	pub async fn root_dir(&mut self) -> Result<Dir<'_, D>, Error<D>> {
		Dir::load(self, 0).await
	}

	pub async fn finish_transaction(&mut self) -> Result<(), Error<D>> {
		self.storage.finish_transaction().await.map_err(Error::Nros)
	}

	pub async fn get_dir(&mut self, id: u64) -> Result<Dir<'_, D>, Error<D>> {
		Dir::load(self, id).await
	}

	async fn read(&mut self, id: u64, offset: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
		self.storage
			.get(id)
			.await?
			.read(offset, buf)
			.await
			.map_err(Error::Nros)
	}

	async fn read_exact(&mut self, id: u64, offset: u64, buf: &mut [u8]) -> Result<(), Error<D>> {
		self.read(id, offset, buf)
			.await
			.and_then(|l| (l == buf.len()).then_some(()).ok_or(Error::Truncated))
	}

	async fn write(&mut self, id: u64, offset: u64, data: &[u8]) -> Result<usize, Error<D>> {
		self.storage
			.get(id)
			.await?
			.write(offset, data)
			.await
			.map_err(Error::Nros)
	}

	async fn write_all(&mut self, id: u64, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		self.write(id, offset, data)
			.await
			.and_then(|l| (l == data.len()).then_some(()).ok_or(Error::Truncated))
	}

	/// This function automatically grows the object if it can't contain the data.
	async fn write_grow(&mut self, id: u64, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		if self.length(id).await? < offset + data.len() as u64 {
			self.resize(id, offset + data.len() as u64).await?;
		}
		self.write_all(id, offset, data).await
	}

	async fn resize(&mut self, id: u64, len: u64) -> Result<(), Error<D>> {
		self.storage
			.get(id)
			.await?
			.resize(len)
			.await
			.map_err(Error::Nros)
	}

	async fn length(&mut self, id: u64) -> Result<u64, Error<D>> {
		self.storage.get(id).await?.len().await.map_err(Error::Nros)
	}

	pub async fn unmount(self) -> Result<Vec<D>, Error<D>> {
		self.storage.unmount().await.map_err(Error::Nros)
	}

	pub fn block_size(&self) -> BlockSize {
		self.storage.block_size()
	}
}

pub enum Error<D>
where
	D: Dev,
{
	Nros(nros::Error<D>),
	Truncated,
	CorruptExtension,
	UnknownHashAlgorithm(u8),
}

impl<D> fmt::Debug for Error<D>
where
	D: Dev,
	D::Error: fmt::Debug,
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

impl<D: Dev> From<nros::Error<D>> for Error<D> {
	fn from(err: nros::Error<D>) -> Self {
		Self::Nros(err)
	}
}
