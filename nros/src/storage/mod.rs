pub mod allocator;
pub mod dev;

use {
	crate::{
		data::record::{self, Depth, RecordRef},
		resource::Buf,
		BlockSize, Compression, Error, KeyDeriver, MaxRecordSize, Resource,
	},
	allocator::Allocator,
	core::cell::{Cell, RefCell, RefMut},
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
pub(crate) struct Store<D: Dev, R: Resource> {
	devices: DevSet<D, R>,
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

impl<D: Dev, R: Resource> Store<D, R> {
	pub async fn new(devices: DevSet<D, R>, allow_repair: bool) -> Result<Self, Error<D>> {
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
	pub async fn read(&self, record_ref: RecordRef) -> Result<R::Buf, Error<D>> {
		if record_ref.blocks() == 0 {
			return Ok(self.devices.resource.alloc());
		}

		let lba = record_ref.lba();
		let blocks = record_ref.blocks();

		#[cfg(debug_assertions)]
		self.allocator.borrow().assert_alloc(lba, blocks.into());

		let count = usize::from(blocks) << self.block_size().to_raw();

		// Attempt to read the record from any chain.
		//
		// If one of the chains fail, try another until we run out.
		// If we run out of chains, return the last error.
		// If we find a successful chain, copy the record to the other chains and log an error.
		let mut blacklist = Set256::default();
		let mut last_err = None;
		let (data, v) = loop {
			let res = self.devices.read(lba, count, &blacklist).await;
			let (mut data, chain) = match res {
				Ok(Some(res)) => res,
				Ok(None) => return Err(last_err.expect("no chains were tried")),
				Err((e, chain)) => {
					blacklist.set(chain, true);
					last_err = Some(e);
					continue;
				}
			};

			let cipher = self.devices.cipher();
			let max_rec_size = self.max_rec_size();
			let buf = self.resource().alloc();
			let entry_data = self
				.resource()
				.run(move || {
					let buf = record::unpack(data.get_mut(), buf, max_rec_size, cipher);
					match buf {
						Ok(buf) => Ok((buf, data)),
						Err(e) => Err((e, data)),
					}
				})
				.await;

			match entry_data {
				Ok((v, data)) => break (data, v),
				Err((e, _)) => {
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
			.update(|x| x + (u64::from(record_ref.blocks()) << self.block_size().to_raw()));
		self.unpacked_bytes_read
			.update(|x| x + u64::try_from(v.len()).unwrap());

		Ok(v)
	}

	/// Write a record.
	pub async fn write(&self, data: R::Buf) -> Result<(RecordRef, R::Buf), Error<D>> {
		assert!(
			data.len() <= 1 << self.max_rec_size().to_raw(),
			"data len is greater than max record size"
		);

		// Calculate minimum size of buffer necessary for the compression algorithm
		// to work.
		let len = self.compression().max_output_size(data.len());
		let max_blks = self.block_size().min_blocks(32 + len);
		let block_count = self.devices.block_count();

		// Allocate and pack record.
		let mut buf = self
			.devices
			.alloc(max_blks << self.block_size().to_raw())
			.await?;
		let compression = self.compression();
		let block_size = self.block_size();
		let data_len = data.len();

		if data_len == 0 {
			// Return empty record.
			return Ok((RecordRef::NONE, data));
		}

		let cipher = self.devices.cipher();
		let nonce = self.devices.gen_nonce();

		let (blks, mut buf, data) = self
			.resource()
			.run(move || {
				let len = record::pack(
					data.get(),
					buf.get_mut(),
					compression,
					block_size,
					cipher,
					nonce,
				);
				(len, buf, data)
			})
			.await;

		// Strip unused blocks from the buffer
		buf.shrink(usize::from(blks) << self.block_size().to_raw());

		// Allocate storage space.
		let lba = self
			.allocator
			.borrow_mut()
			.alloc(blks.into(), block_count)
			.ok_or(Error::NotEnoughSpace)?;

		// Write buffer.
		let rec = RecordRef::new(lba, blks);
		self.devices
			.write(lba.try_into().unwrap(), buf, Set256::set_all())
			.await?;

		self.packed_bytes_written
			.update(|x| x + (u64::from(rec.blocks()) << self.block_size().to_raw()));
		self.unpacked_bytes_written
			.update(|x| x + u64::try_from(data_len).unwrap());

		// Presto!
		Ok((rec, data))
	}

	/// Destroy a record.
	pub fn destroy(&self, record_ref: RecordRef) {
		let blocks = u64::from(record_ref.blocks());
		self.allocator.borrow_mut().free(record_ref.lba(), blocks);
		self.packed_bytes_destroyed
			.update(|x| x + (blocks << self.block_size().to_raw()));
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
	pub async fn unmount(self) -> Result<DevSet<D, R>, Error<D>> {
		self.finish_transaction().await?;
		Ok(self.devices)
	}

	pub fn block_size(&self) -> BlockSize {
		self.devices.block_size()
	}

	pub fn max_rec_size(&self) -> MaxRecordSize {
		self.devices.max_record_size()
	}

	pub fn compression(&self) -> Compression {
		self.devices.compression()
	}

	/// Get the root record of the object list.
	pub fn object_list_root(&self) -> RecordRef {
		self.devices.object_list_root.get()
	}

	/// Set the root record of the object list.
	pub fn set_object_list_root(&self, root: RecordRef) {
		self.devices.object_list_root.set(root)
	}

	/// Get the root record of the object bitmap.
	pub fn object_bitmap_root(&self) -> RecordRef {
		self.devices.object_bitmap_root.get()
	}

	/// Set the root record of the object bitmap.
	pub fn set_object_bitmap_root(&self, root: RecordRef) {
		self.devices.object_bitmap_root.set(root)
	}

	/// Get the depth of the object list.
	pub fn object_list_depth(&self) -> Depth {
		self.devices.object_list_depth.get()
	}

	/// Set the depth of the object list.
	pub fn set_object_list_depth(&self, depth: Depth) {
		self.devices.object_list_depth.set(depth)
	}

	/// Get statistics for this session.
	pub fn statistics(&self) -> Statistics {
		macro_rules! s {
			{$($f:ident)*} => {
				Statistics {
					allocation: self.allocator.borrow().statistics,
					block_size: self.block_size(),
					compression: self.compression(),
					max_record_size: self.max_rec_size(),
					$($f: self.$f.get(),)*
				}
			}
		}
		let mut s = s! {
			packed_bytes_read
			packed_bytes_written
			packed_bytes_destroyed
			unpacked_bytes_read
			unpacked_bytes_written
			device_read_failures
			record_unpack_failures
		};
		s.allocation.total_blocks = self.devices.block_count();
		s
	}

	/// Get the key used to encrypt the header.
	pub fn header_key(&self) -> [u8; 32] {
		self.devices.header_key()
	}

	/// Get reference to filesystem data in the header
	pub fn header_data(&self) -> RefMut<'_, [u8]> {
		RefMut::map(self.devices.data.borrow_mut(), |d| &mut **d)
	}

	/// Set a new key derivation function.
	///
	/// This replaces the header key.
	pub fn set_key_deriver(&self, kdf: KeyDeriver<'_>) {
		self.devices.set_key_deriver(kdf)
	}

	pub fn resource(&self) -> &R {
		&self.devices.resource
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
	/// Size of a single block.
	pub block_size: BlockSize,
	/// Maximum size of a record.
	pub max_record_size: MaxRecordSize,
	/// Default compression to apply to records.
	pub compression: Compression,
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
