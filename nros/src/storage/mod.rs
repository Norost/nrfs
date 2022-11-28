mod allocator;
pub mod dev;

use {
	crate::{header::Header, Compression, Error, LoadError, MaxRecordSize, Record, NewError},
	alloc::vec::Vec,
	allocator::Allocator,
	core::{
		fmt,
		marker::PhantomData,
		ops::{Deref, DerefMut, Range},
	},
	dev::Allocator as _,
};

pub use dev::{Dev, DevSet};

/// A single store of records.
///
/// It manages allocations on the devices and ensures records are mirrored properly.
///
/// It also handles conflicts in block sizes.
///
/// It does *not* handle caching.
/// Records are read and written as a single unit.
#[derive(Debug)]
pub struct Store<D>
where
	D: Dev,
{
	devices: DevSet<D>,
	allocator: Allocator,
	max_record_size: MaxRecordSize,
	block_size_p2: u8,
	compression: Compression,
}

impl<D> Store<D>
where
	D: Dev,
{
	pub fn new(devices: DevSet<D>) -> Result<Self, NewError<D>> {
		todo!()
	}

	pub async fn load(devices: DevSet<D>, record_size: MaxRecordSize, compression: Compression, block_size_p2: u8) -> Result<Self, LoadError<D>> {
		let header = devices.header().await;
		Ok(Self {
			allocator: Allocator::load(&mut devices, header.allocation_log_lba, header.allocation_log_length)?,
			devices,
			max_record_size: record_size,
			compression,
			block_size_p2,
		})
	}

	/// Read a record.
	pub async fn read(&mut self, record: &Record) -> Result<Vec<u8>, Error<D>> {
		if record.lba == 0 {
			debug_assert!(record.length == 0);
			return Ok(None);
		}
		debug_assert!(record.lba != 0);

		let lba = record.lba.into();
		let len = record.length.into();

		let count = self.calc_block_count(len);
		let rd = self.storage.read(lba, count).map_err(Error::Storage)?;
		let mut v = Vec::new();
		record
			.unpack(&rd.get()[..len as _], &mut v, self.max_record_size)
			.map_err(Error::RecordUnpack)?;
		Ok(v)
	}

	/// Write a record.
	pub async fn write(&mut self, record: &Record, data: &[u8]) -> Result<(), Error<D>> {
		let len = self.cache.compression.max_output_size(self.data.len());
		let max_blks = self.cache.calc_block_count(len as _);
		let block_count = self.cache.storage.block_count();
		let mut w = self.cache.storage.write(max_blks).map_err(Error::Storage)?;
		let mut rec = Record::pack(&self.data, w.get_mut(), self.cache.compression);
		let blks = self.calc_block_count(rec.length.into());
		let lba = self
			.cache
			.allocator
			.alloc(blks as _, block_count)
			.ok_or(Error::NotEnoughSpace)?;
		rec.lba = lba.into();
		w.set_region(lba, blks).map_err(Error::Storage)?;
		w.finish().map_err(Error::Storage)?;
		self.cache.cache.insert(lba, self.data);
		self.cache.dirty.insert(lba);
		Ok(rec)
	}

	/// Destroy a record.
	pub fn destroy(&mut self, record: &Record) {
		self.allocator
			.free(record.lba.into(), self.calc_block_count(record.length.into()) as _)
	}

	/// Finish the current transaction.
	///
	/// This saves the allocation log, ensures all writes are committed and makes blocks
	/// freed in this transaction available for the next transaction.
	pub async fn finish_transaction(&mut self) -> Result<(), Error<D>> {
		self.allocator.serialize_full(&mut self.storage)
	}

	fn calc_block_count(&self, len: u32) -> usize {
		let bs = 1 << self.block_size_p2();
		((len + bs - 1) / bs) as _
	}

	pub fn block_size_p2(&self) -> u8 {
		self.block_size_p2
	}

	pub fn max_record_size(&self) -> MaxRecordSize {
		self.max_record_size
	}
}

#[derive(Clone, Copy, Debug)]
pub struct AllocLog {
	pub lba: u64,
	pub len: u64,
}
