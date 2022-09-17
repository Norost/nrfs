//#![cfg_attr(not(test), no_std)]
#![feature(let_else)]

extern crate alloc;

macro_rules! raw {
	($ty:ty) => {
		impl AsRef<[u8; core::mem::size_of::<Self>()]> for $ty {
			fn as_ref(&self) -> &[u8; core::mem::size_of::<Self>()] {
				unsafe { &*(self as *const _ as *const _) }
			}
		}

		impl AsMut<[u8; core::mem::size_of::<Self>()]> for $ty {
			fn as_mut(&mut self) -> &mut [u8; core::mem::size_of::<Self>()] {
				unsafe { &mut *(self as *mut _ as *mut _) }
			}
		}
	};
}

mod allocator;
mod directory;
pub mod header;
mod record;
mod record_cache;
mod record_tree;
mod storage;
#[cfg(test)]
mod test;

pub use storage::{Read, Storage, Write};

use {core::fmt, record::Record, record_cache::RecordCache, record_tree::RecordTree};

pub struct Nros<S: Storage> {
	storage: RecordCache<S>,
	header: header::Header,
}

impl<S: Storage> Nros<S> {
	pub fn new(mut storage: S, max_record_length_p2: u8) -> Result<Self, NewError<S>> {
		// Mandate at least 512 byte blocks since basically every disk has such a minimum size.
		if max_record_length_p2 < 9 {
			return Err(NewError::RecordSizeTooSmall);
		}
		let block_length_p2 = storage.block_size_p2();
		if block_length_p2 < 9 {
			return Err(NewError::BlockTooSmall);
		}

		let h = header::Header { block_length_p2, max_record_length_p2, ..Default::default() };
		let mut w = storage.write(0, 1).map_err(NewError::Storage)?;
		w.get_mut()[..h.as_ref().len()].copy_from_slice(h.as_ref());
		drop(w);
		Ok(Self { storage: RecordCache::new(storage, h.max_record_length_p2), header: h })
	}

	pub fn load(mut storage: S) -> Result<Self, LoadError<S>> {
		let r = storage.read(0, 1).map_err(LoadError::Storage)?;
		let mut header = header::Header::default();
		let l = header.as_ref().len();
		header.as_mut().copy_from_slice(&r.get()[..l]);
		drop(r);
		if header.magic != *b"Nora Reliable FS" {
			return Err(LoadError::InvalidMagic);
		}
		// Mandate at least 128 byte records, otherwise it's impossible to construct a record tree.
		if header.max_record_length_p2 < 7 {
			return Err(LoadError::RecordSizeTooSmall);
		}
		Ok(Self { storage: RecordCache::new(storage, header.max_record_length_p2), header })
	}

	pub fn new_object(&mut self) -> Result<u64, Error<S>> {
		let r = &mut self.header.object_list;
		let l = r.len();
		r.write(&mut self.storage, l, Record::default().as_ref())?;
		Ok(l / 64)
	}

	pub fn move_object(&mut self, to_id: u64, from_id: u64) -> Result<(), Error<S>> {
		let r = self.object_root(from_id)?;
		self.set_object_root(to_id, &r)?;
		self.set_object_root(from_id, &Default::default())
	}

	pub fn object_count(&mut self) -> u64 {
		self.header.object_list.len() / 64
	}

	pub fn object_len(&mut self, id: u64) -> Result<u64, Error<S>> {
		Ok(self.object_root(id)?.len())
	}

	pub fn read_object(&mut self, id: u64, offset: u64, buf: &mut [u8]) -> Result<usize, Error<S>> {
		let obj = self.object_root(id)?;
		if offset >= obj.len() {
			return Ok(0);
		}
		let Some(l) = obj.len().checked_sub(offset) else { return Ok(0) };
		let l = (buf.len() as u64).min(l);
		let buf = &mut buf[..l as _];
		obj.read(&mut self.storage, offset, buf)?;
		Ok(buf.len())
	}

	pub fn write_object(&mut self, id: u64, offset: u64, data: &[u8]) -> Result<(), Error<S>> {
		let mut rec = self.object_root(id)?;
		rec.write(&mut self.storage, offset, data)?;
		self.header
			.object_list
			.write(&mut self.storage, id * 64, rec.0.as_ref())?;
		Ok(())
	}

	pub fn truncate_object(&mut self, id: u64, len: u64) -> Result<(), Error<S>> {
		let mut rec = self.object_root(id)?;
		rec.truncate(&mut self.storage, len)?;
		self.header
			.object_list
			.write(&mut self.storage, id * 64, rec.0.as_ref())?;
		Ok(())
	}

	pub fn finish_transaction(&mut self) -> Result<(), Error<S>> {
		let mut w = self.storage.storage.write(0, 1).map_err(Error::Storage)?;
		w.set_blocks(1);
		let (a, b) = w.get_mut().split_at_mut(self.header.as_ref().len());
		a.copy_from_slice(self.header.as_ref());
		b.fill(0);
		Ok(())
	}

	fn object_root(&mut self, id: u64) -> Result<record_tree::RecordTree, Error<S>> {
		let mut rec = record::Record::default();
		self.header
			.object_list
			.read(&mut self.storage, id * 64, rec.as_mut())?;
		Ok(record_tree::RecordTree(rec))
	}

	fn set_object_root(&mut self, id: u64, rec: &record_tree::RecordTree) -> Result<(), Error<S>> {
		self.header
			.object_list
			.write(&mut self.storage, id * 64, rec.0.as_ref())
	}
}

impl<S: Storage> fmt::Debug for Nros<S> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Nros))
			.field("header", &self.header)
			.finish_non_exhaustive()
	}
}

#[derive(Debug)]
pub enum NewError<S: Storage> {
	RecordSizeTooSmall,
	BlockTooSmall,
	Storage(S::Error),
}

#[derive(Debug)]
pub enum LoadError<S: Storage> {
	InvalidMagic,
	RecordSizeTooSmall,
	Storage(S::Error),
}

pub enum Error<S: Storage> {
	Storage(S::Error),
	RecordPack(record::PackError),
	RecordUnpack(record::UnpackError),
	NotEnoughSpace,
}

impl<S: Storage> fmt::Debug for Error<S>
where
	S::Error: fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Storage(e) => f.debug_tuple("Storage").field(&e).finish(),
			Self::RecordPack(e) => f.debug_tuple("RecordPack").field(&e).finish(),
			Self::RecordUnpack(e) => f.debug_tuple("RecordUnpack").field(&e).finish(),
			Self::NotEnoughSpace => f.debug_tuple("NotEnoughSpace").finish(),
		}
	}
}
