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

pub use storage::{Read, Storage, Write};

use {core::fmt, record::Record, record_cache::RecordCache, record_tree::RecordTree};

pub struct Nrfs<S: Storage> {
	storage: RecordCache<S>,
	header: header::Header,
}

impl<S: Storage> Nrfs<S> {
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

	pub fn object_count(&mut self) -> u64 {
		self.header.object_list.len() / 64
	}

	pub fn object_len(&mut self, id: u64) -> Result<u64, Error<S>> {
		Ok(self.object_root(id)?.len())
	}

	pub fn read_object(&mut self, id: u64, offset: u64, buf: &mut [u8]) -> Result<usize, Error<S>> {
		self.object_root(id)?.read(&mut self.storage, offset, buf)
	}

	pub fn write_object(&mut self, id: u64, offset: u64, data: &[u8]) -> Result<usize, Error<S>> {
		let mut rec = self.object_root(id)?;
		let l = rec.write(&mut self.storage, offset, data)?;
		eprintln!("{:02x?}", rec.0.as_ref());
		self.header
			.object_list
			.write(&mut self.storage, id * 64, rec.0.as_ref())?;
		Ok(l)
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
		let l = self
			.header
			.object_list
			.read(&mut self.storage, id * 64, rec.as_mut())?;
		assert_eq!(l, 64);
		Ok(record_tree::RecordTree(rec))
	}
}

impl<S: Storage> fmt::Debug for Nrfs<S> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Nrfs))
			.field("header", &self.header)
			.finish_non_exhaustive()
	}
}

#[derive(Debug)]
pub enum LoadError<S: Storage> {
	InvalidMagic,
	RecordSizeTooSmall,
	Storage(S::Error),
}

#[derive(Debug)]
pub enum Error<S: Storage> {
	Storage(S::Error),
	RecordPack(record::PackError),
	RecordUnpack(record::UnpackError),
	NotEnoughSpace,
}
