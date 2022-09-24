//#![cfg_attr(not(test), no_std)]
#![deny(unused_must_use)]

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
pub mod storage;
#[cfg(test)]
mod test;
mod util;
mod write_buffer;

pub use {
	record::{Compression, MaxRecordSize},
	storage::{Read, Storage, Write},
};

use {
	core::fmt, rangemap::RangeSet, record::Record, record_cache::RecordCache,
	record_tree::RecordTree, std::collections::BTreeMap, write_buffer::WriteBuffer,
};

pub struct Nros<S: Storage> {
	storage: RecordCache<S>,
	header: header::Header,
	used_objects: RangeSet<u64>,
	/// Write buffer for the object list.
	object_list_wb: WriteBuffer,
}
impl<S: Storage> Nros<S> {
	pub fn new(
		mut storage: S,
		max_record_size: MaxRecordSize,
		compression: Compression,
		cache_size: u16,
	) -> Result<Self, NewError<S>> {
		// Mandate at least 512 byte blocks since basically every disk has such a minimum size.
		let block_length_p2 = storage.block_size_p2();
		if block_length_p2 < 9 {
			return Err(NewError::BlockTooSmall);
		}

		let h = header::Header {
			block_length_p2,
			max_record_length_p2: max_record_size.to_raw(),
			compression: compression.to_raw(),
			..Default::default()
		};
		let mut w = storage.write(1).map_err(NewError::Storage)?;
		w.get_mut()[..h.as_ref().len()].copy_from_slice(h.as_ref());
		w.set_region(0, 1).map_err(NewError::Storage)?;
		w.finish().map_err(NewError::Storage)?;
		Ok(Self {
			storage: RecordCache::new(storage, max_record_size, compression, cache_size),
			object_list_wb: WriteBuffer::new(&h.object_list),
			header: h,
			used_objects: Default::default(),
		})
	}

	pub fn load(mut storage: S, cache_size: u16) -> Result<Self, LoadError<S>> {
		let r = storage.read(0, 1).map_err(LoadError::Storage)?;
		let mut h = header::Header::default();
		let l = h.as_ref().len();
		h.as_mut().copy_from_slice(&r.get()[..l]);
		drop(r);
		if h.magic != *b"Nora Reliable FS" {
			return Err(LoadError::InvalidMagic);
		}

		let rec_size = MaxRecordSize::from_raw(h.max_record_length_p2)
			.ok_or(LoadError::InvalidRecordSize(h.max_record_length_p2))?;
		let compr = Compression::from_raw(h.compression)
			.ok_or(LoadError::UnsupportedCompression(h.compression))?;
		let mut storage = RecordCache::load(
			storage,
			rec_size,
			h.allocation_log_lba.into(),
			h.allocation_log_length.into(),
			compr,
			cache_size,
		)?;

		let max_obj_id_used = h.object_list.len() / 64;
		let mut used_objects = RangeSet::from_iter([0..max_obj_id_used + 1]);
		for id in 0..max_obj_id_used + 1 {
			let mut rec = Record::default();
			h.object_list
				.read(&mut storage, id * 64, rec.as_mut())
				.map_err(|e| match e {
					_ => todo!(),
				})?;
			if rec.reference_count == 0 {
				used_objects.remove(id..id + 1);
			}
		}

		Ok(Self {
			storage,
			object_list_wb: WriteBuffer::new(&h.object_list),
			header: h,
			used_objects,
		})
	}

	fn alloc_ids(&mut self, count: u64) -> u64 {
		for r in self.used_objects.gaps(&(0..u64::MAX)) {
			if r.end - r.start >= count {
				self.used_objects.insert(r.start..r.start + count);
				return r.start;
			}
		}
		unreachable!("more than 2**64 objects allocated");
	}

	fn dealloc_id(&mut self, id: u64) {
		debug_assert!(self.used_objects.contains(&id), "double free");
		self.used_objects.remove(id..id + 1);
	}

	pub fn new_object(&mut self) -> Result<u64, Error<S>> {
		let id = self.alloc_ids(1);
		let r = &mut self.object_list_wb;
		if r.len() < id * 64 + 64 {
			r.resize(id * 64 + 64);
		}
		r.write(
			&mut self.storage,
			&self.header.object_list,
			id * 64,
			Record { reference_count: 1, ..Default::default() }.as_ref(),
		)?;
		Ok(id)
	}

	/// Return IDs for two objects, one at ID and one at ID + 1
	pub fn new_object_pair(&mut self) -> Result<u64, Error<S>> {
		let id = self.alloc_ids(2);
		let w = &mut self.object_list_wb;
		if w.len() <= id * 64 + 128 {
			w.resize(id * 64 + 128);
		}
		let rec = Record { reference_count: 1, ..Default::default() };
		let mut b = [0; 128];
		b[..64].copy_from_slice(rec.as_ref());
		b[64..].copy_from_slice(rec.as_ref());
		w.write(&mut self.storage, &self.header.object_list, id * 64, &b)?;
		Ok(id)
	}

	/// Decrement the reference count to an object.
	///
	/// If this count reaches zero the object is automatically freed.
	///
	/// This function *must not* be used on invalid objects!
	pub fn decr_ref(&mut self, id: u64) -> Result<(), Error<S>> {
		let mut obj = self.object_root(id)?;
		obj.0.reference_count -= 1;
		if obj.0.reference_count == 0 {
			obj.resize(&mut self.storage, 0)?;
			self.dealloc_id(id);
		}
		self.set_object_root(id, &obj)
	}

	pub fn move_object(&mut self, to_id: u64, from_id: u64) -> Result<(), Error<S>> {
		self.object_root(to_id)?.resize(&mut self.storage, 0)?;
		let f = self.object_root(from_id)?;
		self.set_object_root(to_id, &f)?;
		self.set_object_root(from_id, &Default::default())?;
		self.dealloc_id(from_id);
		Ok(())
	}

	pub fn object_len(&mut self, id: u64) -> Result<u64, Error<S>> {
		Ok(self.object_root(id)?.len())
	}

	pub fn read(&mut self, id: u64, offset: u64, buf: &mut [u8]) -> Result<usize, Error<S>> {
		let obj = self.object_root(id)?;
		if offset >= obj.len() {
			return Ok(0);
		}
		let l = if let Some(l) = obj.len().checked_sub(offset) {
			l
		} else {
			return Ok(0);
		};
		let l = (buf.len() as u64).min(l);
		let buf = &mut buf[..l as _];
		obj.read(&mut self.storage, offset, buf)?;
		Ok(buf.len())
	}

	pub fn write(&mut self, id: u64, offset: u64, data: &[u8]) -> Result<usize, Error<S>> {
		let mut obj = self.object_root(id)?;
		if offset >= obj.len() {
			return Ok(0);
		}
		let l = if let Some(l) = obj.len().checked_sub(offset) {
			l
		} else {
			return Ok(0);
		};
		let l = (data.len() as u64).min(l);
		let data = &data[..l as _];
		obj.write(&mut self.storage, offset, data)?;
		self.object_list_wb.write(
			&mut self.storage,
			&self.header.object_list,
			id * 64,
			obj.0.as_ref(),
		)?;
		Ok(data.len())
	}

	pub fn resize(&mut self, id: u64, len: u64) -> Result<(), Error<S>> {
		let mut rec = self.object_root(id)?;
		rec.resize(&mut self.storage, len)?;
		self.object_list_wb.write(
			&mut self.storage,
			&self.header.object_list,
			id * 64,
			rec.0.as_ref(),
		)?;
		Ok(())
	}

	pub fn finish_transaction(&mut self) -> Result<(), Error<S>> {
		// Flush write buffers
		self.object_list_wb.flush(&mut self.storage, &mut self.header.object_list)?;

		// Save allocation log
		let (lba, len) = self.storage.finish_transaction()?;
		self.header.allocation_log_lba = lba.into();
		self.header.allocation_log_length = len.into();

		// Write header
		let mut w = self.storage.storage.write(1).map_err(Error::Storage)?;
		w.set_region(0, 1).map_err(Error::Storage)?;
		let (a, b) = w.get_mut().split_at_mut(self.header.as_ref().len());
		a.copy_from_slice(self.header.as_ref());
		b.fill(0);
		w.finish().map_err(Error::Storage)
	}

	/// This function *must not* be used on invalid objects!
	fn object_root(&mut self, id: u64) -> Result<record_tree::RecordTree, Error<S>> {
		let w = &mut self.object_list_wb;
		debug_assert!(id * 64 < w.len());
		let mut rec = RecordTree::default();
		w.read(
			&mut self.storage,
			&self.header.object_list,
			id * 64,
			rec.0.as_mut(),
		)?;
		debug_assert!(rec.0.reference_count > 0, "invalid object {}", id);
		Ok(rec)
	}

	fn set_object_root(&mut self, id: u64, rec: &record_tree::RecordTree) -> Result<(), Error<S>> {
		self.object_list_wb
			.write(&mut self.storage, &self.header.object_list, id * 64, rec.0.as_ref())
	}

	pub fn storage(&self) -> &S {
		&self.storage.storage
	}
}

impl<S: Storage> fmt::Debug for Nros<S> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Nros))
			.field("header", &self.header)
			.field("storage", &self.storage)
			.finish_non_exhaustive()
	}
}

#[derive(Debug)]
pub enum NewError<S: Storage> {
	BlockTooSmall,
	Storage(S::Error),
}

#[derive(Debug)]
pub enum LoadError<S: Storage> {
	InvalidMagic,
	InvalidRecordSize(u8),
	UnsupportedCompression(u8),
	Storage(S::Error),
}

pub enum Error<S: Storage> {
	Storage(S::Error),
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
			Self::RecordUnpack(e) => f.debug_tuple("RecordUnpack").field(&e).finish(),
			Self::NotEnoughSpace => f.debug_tuple("NotEnoughSpace").finish(),
		}
	}
}
