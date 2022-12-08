mod allocator;
pub mod dev;

use {
	crate::{BlockSize, Compression, Error, MaxRecordSize, Record},
	alloc::vec::Vec,
	allocator::Allocator,
	core::cell::RefCell,
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
	allocator: RefCell<Allocator>,
}

impl<D> Store<D>
where
	D: Dev,
{
	pub async fn new(devices: DevSet<D>) -> Result<Self, Error<D>> {
		Ok(Self { allocator: Allocator::load(&devices).await?.into(), devices })
	}

	/// Read a record.
	pub async fn read(&self, record: &Record) -> Result<Vec<u8>, Error<D>> {
		if record.length == 0 {
			return Ok(Vec::new());
		}

		let lba = u64::from(record.lba);
		let len = record.length.into();

		let count = self.calc_block_count(len) << self.block_size();
		let data = self
			.devices
			.read(lba.try_into().unwrap(), count, Default::default())
			.await?;
		let mut v = Vec::new();
		record
			.unpack(&data.get()[..len as _], &mut v, self.max_record_size())
			.map_err(Error::RecordUnpack)?;
		Ok(v)
	}

	/// Write a record.
	pub async fn write(&self, data: &[u8]) -> Result<Record, Error<D>> {
		// Calculate minimum size of buffer necessary for the compression algorithm
		// to work.
		let len = self.compression().max_output_size(data.len());
		let max_blks = self.calc_block_count(len as _);
		let block_count = self.devices.block_count();

		// Allocate and pack record.
		let mut buf = self
			.devices
			.alloc(max_blks << self.block_size().to_raw())
			.await?;
		let mut rec = Record::pack(&data, buf.get_mut(), self.compression());

		// Strip unused blocks from the buffer
		let blks = self.calc_block_count(rec.length.into());
		if blks == 0 {
			// Return empty record.
			return Ok(Record::default());
		}
		buf.shrink(blks << self.block_size().to_raw());

		// Allocate storage space.
		let lba = self
			.allocator
			.borrow_mut()
			.alloc(blks as _, block_count)
			.ok_or(Error::NotEnoughSpace)?;

		// Write buffer.
		rec.lba = lba.into();
		self.devices.write(lba.try_into().unwrap(), buf).await?;

		// Presto!
		Ok(rec)
	}

	/// Destroy a record.
	pub fn destroy(&self, record: &Record) {
		self.allocator.borrow_mut().free(
			record.lba.into(),
			self.calc_block_count(record.length.into()) as _,
		)
	}

	/// Finish the current transaction.
	///
	/// This saves the allocation log, ensures all writes are committed and makes blocks
	/// freed in this transaction available for the next transaction.
	pub async fn finish_transaction(&self) -> Result<(), Error<D>> {
		self.allocator
			.borrow_mut()
			.save(&self.devices, self.max_record_size())
			.await
	}

	fn calc_block_count(&self, len: u32) -> usize {
		let bs = 1 << self.block_size().to_raw();
		((len + bs - 1) / bs) as _
	}

	pub fn block_size(&self) -> BlockSize {
		self.devices.block_size()
	}

	pub fn max_record_size(&self) -> MaxRecordSize {
		self.devices.max_record_size()
	}

	pub fn compression(&self) -> Compression {
		self.devices.compression()
	}

	/// Get the root record of the object list.
	pub fn object_list(&self) -> Record {
		self.devices.object_list.get()
	}

	/// Set the root record of the object list.
	pub fn set_object_list(&self, root: Record) {
		self.devices.object_list.set(root)
	}
}

#[derive(Clone, Copy, Debug)]
pub struct AllocLog {
	pub lba: u64,
	pub len: u64,
}
