use {
	super::{Dev, Store},
	crate::{util, Error, Record},
	core::mem,
	endian::u64le,
	futures_util::stream::{FuturesUnordered, TryStreamExt},
	rangemap::RangeSet,
};

#[derive(Clone, Copy, Debug, Default)]
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
	/// Previously allocated stack records.
	///
	/// Should be freed on log rewrite.
	stack: Vec<Record>,
}

impl Allocator {
	pub async fn load<D>(store: &Store<D>) -> Result<Self, Error<D>>
	where
		D: Dev,
	{
		let mut alloc_map = RangeSet::new();
		let mut stack = Vec::new();

		// Iterate stack from top to bottom
		let mut record = store.devices.allocation_log.get();
		let mut ignore = RangeSet::new(); // ranges that are already covered by a recent entry.
		while record.length > 0 {
			// Append to stack for later ops
			stack.push(record);

			// Get record data
			let data = store.read(&record).await?;
			let lba = u64::from(record.lba);
			let size = u64::from(u32::from(record.length));
			let end = usize::try_from(size).unwrap();

			// Add record itself
			alloc_map.insert(lba..lba + size);
			ignore.insert(lba..lba + size);

			// Add entries
			let entry_mask = mem::size_of::<Entry>() - 1;
			for i in (mem::size_of::<Record>()..(end + entry_mask) & !entry_mask)
				.step_by(16)
				.rev()
			{
				// Get entry
				let max_len = (end - i).min(mem::size_of::<Entry>());
				let mut entry = Entry::default();
				entry.as_mut()[..max_len].copy_from_slice(&data[i..][..max_len]);

				let alloc = entry.size & 1 << 63 == 0;
				let size = entry.size & ((1 << 63) - 1);
				let range = u64::from(entry.lba)..u64::from(entry.lba) + size;

				// Only fill in gaps that haven't got a more recent log entry.
				for gap in ignore.gaps(&range) {
					if alloc {
						alloc_map.insert(gap);
					} else {
						alloc_map.remove(gap);
					}
				}

				// Ignore in later entries.
				ignore.insert(range);
			}

			// Next record
			record = util::get_record(&data, 0);
		}

		Ok(Self { alloc_map, free_map: Default::default(), dirty_map: Default::default(), stack })
	}

	pub fn alloc(&mut self, blocks: u64, block_count: u64) -> Option<u64> {
		if blocks == 0 {
			return Some(0);
		}
		for r in self.alloc_map.gaps(&(0..block_count)) {
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
	pub async fn save<D>(&mut self, store: &Store<D>) -> Result<(), Error<D>>
	where
		D: Dev,
	{
		// Update map
		// TODO it would be nice if we could avoid a Clone.
		let mut alloc_map = self.alloc_map.clone();
		for r in self.free_map.iter() {
			alloc_map.remove(r.clone());
		}

		// Save map
		// TODO avoid writing the entire log every time.

		// Deallocate all stack records of current log.
		for record in self.stack.drain(..) {
			let lba = u64::from(record.lba);
			let blocks = store.calc_block_count(record.length.into());
			alloc_map.remove(lba..lba + u64::try_from(blocks).unwrap());
		}

		let mut iter = alloc_map.iter().peekable();
		let rec_size = 1usize << store.max_record_size();
		let entries_per_record = (rec_size - mem::size_of::<Record>()) / mem::size_of::<Entry>();

		// Perform writes concurrently to speed things up a bit.
		let writes = FuturesUnordered::new();

		let mut prev = Record::default();
		let mut buf = Vec::with_capacity(rec_size);
		while iter.peek().is_some() {
			// Reference previous record
			buf.clear();
			buf.extend_from_slice(prev.as_ref());

			// Store entries
			for entry in (&mut iter).take(entries_per_record) {
				let entry =
					Entry { lba: entry.start.into(), size: (entry.end - entry.start).into() };
				assert!(
					entry.size < 1 << 63,
					"size overflow, disks are too massive? :P"
				);
				buf.extend_from_slice(entry.as_ref());
			}

			// Pack record
			util::trim_zeros_end(&mut buf);
			debug_assert!(
				!buf.is_empty(),
				"buffer should have at least one log entry with non-zero size"
			);
			let len = store.round_block_size(buf.len().try_into().unwrap());

			// FIXME we should poll writes while waiting for an alloc,
			// as it is possible all memory is used up by the current writes.
			let mut b = store.devices.alloc(len).await?;

			prev = Record::pack(&buf, b.get_mut(), store.compression());
			let len = store.round_block_size(prev.length.into());
			b.shrink(len);

			// Store record
			let blocks = store
				.calc_block_count(len.try_into().unwrap())
				.try_into()
				.unwrap();
			let lba = self
				.alloc(blocks, store.devices.block_count())
				.ok_or(Error::NotEnoughSpace)?;
			prev.lba = lba.into();
			writes.push(store.devices.write(lba, b));
			self.stack.push(prev);
		}

		// Finish writes
		writes.try_collect().await?;
		store.devices.allocation_log.set(prev);

		// Update alloc_map with *implicitly* recorded allocations for stack records.
		for record in self.stack.iter() {
			let lba = u64::from(record.lba);
			let blocks = store.calc_block_count(record.length.into());
			alloc_map.insert(lba..lba + u64::try_from(blocks).unwrap());
		}

		// Clear free & dirty ranges.
		self.alloc_map = alloc_map;
		self.free_map = Default::default();
		self.dirty_map = Default::default();

		Ok(())
	}
}
