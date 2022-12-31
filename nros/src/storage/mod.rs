pub mod allocator;
pub mod dev;

use {
	crate::{BlockSize, Compression, Error, MaxRecordSize, Record},
	alloc::vec::Vec,
	allocator::Allocator,
	core::cell::{Cell, RefCell},
	dev::Set256,
};

pub(crate) use dev::DevSet;

pub use dev::Dev;

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

	/// Packed bytes read.
	packed_bytes_read: Cell<u64>,
	/// Packed bytes written.
	packed_bytes_written: Cell<u64>,
	/// Packed bytes destroyed.
	packed_bytes_destroyed: Cell<u64>,
	/// Unpacked bytes read.
	unpacked_bytes_read: Cell<u64>,
	/// Unpacked bytes written.
	unpacked_bytes_written: Cell<u64>,
	/// Amount of device read failures.
	device_read_failures: Cell<u64>,
	/// Amount of record unpack failures.
	record_unpack_failures: Cell<u64>,

	/// Whether to repair broken records or not.
	allow_repair: bool,
}

impl<D> Store<D>
where
	D: Dev,
{
	pub async fn new(devices: DevSet<D>, allow_repair: bool) -> Result<Self, Error<D>> {
		let mut slf = Self {
			allocator: Default::default(),
			devices,
			packed_bytes_read: Default::default(),
			packed_bytes_written: Default::default(),
			packed_bytes_destroyed: Default::default(),
			unpacked_bytes_read: Default::default(),
			unpacked_bytes_written: Default::default(),
			device_read_failures: Default::default(),
			record_unpack_failures: Default::default(),
			allow_repair,
		};
		slf.allocator = Allocator::load(&slf).await?.into();
		Ok(slf)
	}

	/// Read a record.
	pub async fn read(&self, record: &Record) -> Result<Vec<u8>, Error<D>> {
		if record.length == 0 {
			return Ok(Vec::new());
		}

		let lba = u64::from(record.lba);
		let len = record.length.into();

		let blocks = self.calc_block_count(len);
		#[cfg(debug_assertions)]
		self.allocator
			.borrow()
			.assert_alloc(lba, blocks.try_into().unwrap());

		let count = blocks << self.block_size();

		// Attempt to read the record from any chain.
		//
		// If one of the chains fail, try another until we run out.
		// If we run out of chains, return the last error.
		// If we find a successful chain, copy the record to the other chains and log an error.
		let mut v = Vec::new();
		let mut blacklist = Set256::default();
		let mut last_err = None;
		let data = loop {
			let res = self
				.devices
				.read(lba.try_into().unwrap(), count, &blacklist)
				.await;
			let (data, chain) = match res {
				Ok(Some(res)) => res,
				Ok(None) => return Err(last_err.expect("no chains were tried")),
				Err((e, chain)) => {
					blacklist.set(chain, true);
					last_err = Some(e);
					continue;
				}
			};
			match record.unpack(&data.get()[..len as _], &mut v, self.max_record_size()) {
				Ok(()) => break data,
				Err(e) => {
					self.record_unpack_failures.update(|x| x + 1);
					blacklist.set(chain, true);
					last_err = Some(Error::RecordUnpack(e));
				}
			}
		};
		if self.allow_repair {
			// Write to all devices where failure was encountered.
			self.devices.write(lba, data, blacklist).await?;
		}

		self.packed_bytes_read
			.update(|x| x + u64::from(u32::from(record.length)));
		self.unpacked_bytes_read
			.update(|x| x + u64::try_from(v.len()).unwrap());

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
		let mut rec = Record::pack(data, buf.get_mut(), self.compression(), self.block_size());

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
		self.devices
			.write(lba.try_into().unwrap(), buf, Set256::set_all())
			.await?;

		self.packed_bytes_written
			.update(|x| x + u64::from(u32::from(rec.length)));
		self.unpacked_bytes_written
			.update(|x| x + u64::try_from(data.len()).unwrap());

		// Presto!
		Ok(rec)
	}

	/// Destroy a record.
	pub fn destroy(&self, record: &Record) {
		self.allocator.borrow_mut().free(
			record.lba.into(),
			self.calc_block_count(record.length.into()) as _,
		);
		self.packed_bytes_destroyed
			.update(|x| x + u64::from(u32::from(record.length)));
	}

	/// Finish the current transaction.
	///
	/// This saves the allocation log, ensures all writes are committed and makes blocks
	/// freed in this transaction available for the next transaction.
	pub async fn finish_transaction(&self) -> Result<(), Error<D>> {
		self.allocator.borrow_mut().save(self).await?;
		self.devices.save_headers().await
	}

	/// Unmount the object store.
	///
	/// The current transaction is finished before returning the [`DevSet`].
	pub async fn unmount(self) -> Result<DevSet<D>, Error<D>> {
		self.finish_transaction().await?;
		Ok(self.devices)
	}

	fn calc_block_count(&self, len: u32) -> usize {
		let bs = 1 << self.block_size().to_raw();
		((len + bs - 1) / bs).try_into().unwrap()
	}

	fn round_block_size(&self, len: u32) -> usize {
		let bs = 1 << self.block_size().to_raw();
		((len + bs - 1) & !(bs - 1)).try_into().unwrap()
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

	/// Get statistics for this session.
	pub fn statistics(&self) -> Statistics {
		macro_rules! s {
			{$($f:ident)*} => {
				Statistics {
					allocation: self.allocator.borrow().statistics,
					$($f: self.$f.get(),)*
				}
			}
		}
		s! {
			packed_bytes_read
			packed_bytes_written
			packed_bytes_destroyed
			unpacked_bytes_read
			unpacked_bytes_written
			device_read_failures
			record_unpack_failures
		}
	}

	/// Ensure all blocks in a range are allocated.
	///
	/// Used to detect use-after-frees.
	#[cfg(debug_assertions)]
	pub fn assert_alloc(&self, record: &Record) {
		if record.length > 0 {
			let blocks = self
				.calc_block_count(record.length.into())
				.try_into()
				.unwrap();
			self.allocator
				.borrow_mut()
				.assert_alloc(record.lba.into(), blocks)
		}
	}
}

#[derive(Clone, Copy, Debug)]
pub struct AllocLog {
	pub lba: u64,
	pub len: u64,
}

/// Statistics for this session.
///
/// Used for debugging.
#[derive(Clone, Copy, Debug, Default)]
pub struct Statistics {
	/// Allocation statistics.
	pub allocation: allocator::Statistics,
	/// Packed bytes read.
	pub packed_bytes_read: u64,
	/// Packed bytes written.
	pub packed_bytes_written: u64,
	/// Packed bytes destroyed.
	pub packed_bytes_destroyed: u64,
	/// Unpacked bytes read.
	pub unpacked_bytes_read: u64,
	/// Unpacked bytes written.
	pub unpacked_bytes_written: u64,
	/// Amount of device read failures.
	pub device_read_failures: u64,
	/// Amount of record unpack failures.
	pub record_unpack_failures: u64,
}
