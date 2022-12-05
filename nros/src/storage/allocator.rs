use {
	super::{Dev, DevSet},
	crate::{BlockSize, Buf, Error, LoadError, MaxRecordSize},
	endian::u64le,
	rangemap::RangeSet,
};

#[repr(C, align(16))]
struct Entry {
	lba: u64le,
	size: u64le,
}

raw!(Entry);

#[derive(Debug, Default)]
pub struct Allocator {
	/// Map of *allocated* blocks.
	///
	/// These can be freely used.
	alloc_map: RangeSet<u64>,
	/// Map of blocks previously allocated but now freed.
	///
	/// These cannot be used until the current transaction finishes.
	free_map: RangeSet<u64>,
	/// Map of blocks previously free but now allocated.
	///
	/// This is used to determine whether a block can safely be recycled in the current
	/// transaction.
	dirty_map: RangeSet<u64>,
}

impl Allocator {
	pub async fn load<D>(devices: &DevSet<D>) -> Result<Self, Error<D>>
	where
		D: Dev,
	{
		let (lba, len) = (devices.allocation_log_lba.get(), devices.allocation_log_length.get());

		let mut alloc_map = RangeSet::new();

		if len > 0 {
			let block_size = 1 << devices.block_size().to_raw();
			let blocks = (len + block_size - 1) / block_size;
			let rd = devices.read(lba, blocks.try_into().unwrap()).await?;
			for r in rd.get()[..len as _].chunks_exact(16) {
				let start = u64::from_le_bytes(r[..8].try_into().unwrap());
				let len = u64::from_le_bytes(r[8..].try_into().unwrap());
				if len & !(1 << 63) == 0 {
					continue;
				}
				if len & 1 << 63 != 0 {
					alloc_map.remove(start..start + len ^ 1 << 63);
				} else {
					alloc_map.insert(start..start + len);
				}
			}
		}

		Ok(Self { alloc_map, free_map: Default::default(), dirty_map: Default::default() })
	}

	pub fn alloc(&mut self, blocks: u64, block_count: u64) -> Option<u64> {
		if blocks == 0 {
			return Some(0);
		}
		for r in self.alloc_map.gaps(&(1..block_count)) {
			if r.end - r.start >= blocks {
				self.alloc_map.insert(r.start..r.start + blocks);
				self.dirty_map.insert(r.start..r.start + blocks);
				return Some(r.start);
			}
		}
		None
	}

	pub fn free(&mut self, start: u64, blocks: u64) {
		// TODO RangeSet panics if blocks == 0.
		// It would make more sense if it just ignored the range.
		if blocks == 0 {
			return;
		}
		// FIXME really stupid
		for i in start..start + blocks {
			if self.dirty_map.contains(&i) {
				self.dirty_map.remove(i..i + 1);
				self.alloc_map.remove(i..i + 1);
			} else {
				self.free_map.insert(i..i + 1);
			}
		}
	}

	/// Save the allocator state.
	pub async fn save<D>(
		&mut self,
		devs: &DevSet<D>,
		record_size: MaxRecordSize,
	) -> Result<(), Error<D>>
	where
		D: Dev,
	{
		// Update map
		// TODO it would be nice if we could avoid a Clone.
		let mut alloc_map = self.alloc_map.clone();
		for r in self.free_map.iter() {
			alloc_map.remove(r.clone());
		}

		// Allocate space for all entries + 1
		let (len_min, len_max) = alloc_map.iter().size_hint();
		assert_eq!(Some(len_min), len_max);
		let len = (len_min as u64 + 1) * 16;
		assert!(len <= 1 << record_size.to_raw(), "todo: multiple records");
		let blocks = (len + (1 << devs.block_size().to_raw()) - 1) >> devs.block_size().to_raw();
		let lba = self
			.alloc(blocks, devs.block_count())
			.ok_or(Error::NotEnoughSpace)?;

		// Save map
		let mut buf = devs
			.alloc(usize::try_from(blocks).unwrap() << devs.block_size().to_raw())
			.await?;
		for (w, r) in buf
			.get_mut()
			.chunks_exact_mut(16)
			.zip(self.alloc_map.iter())
		{
			let len = r.end - r.start;
			w[..8].copy_from_slice(&r.start.to_le_bytes());
			w[8..].copy_from_slice(&len.to_le_bytes());
		}
		devs.write(lba, buf).await?;

		self.alloc_map = alloc_map;
		self.free_map = Default::default();
		self.dirty_map = Default::default();

		devs.allocation_log_lba.set(lba);
		devs.allocation_log_length.set(len);

		Ok(())
	}
}
