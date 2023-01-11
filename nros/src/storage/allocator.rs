use {
	super::{Dev, Set256, Store},
	crate::{resource::Buf, util, Error, Record, Resource},
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

#[derive(Debug)]
pub(super) struct Allocator {
	/// Map of *allocated* blocks.
	///
	/// Gaps can be freely used.
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
	/// Allocator statistics.
	///
	/// Used for debugging.
	pub(super) statistics: Statistics,
	#[cfg(feature = "debug-trace-alloc")]
	debug_alloc_traces: rustc_hash::FxHashMap<u64, std::backtrace::Backtrace>,
	#[cfg(feature = "debug-trace-alloc")]
	debug_dealloc_traces: rustc_hash::FxHashMap<u64, std::backtrace::Backtrace>,
}

/// Statistics for this session.
///
/// Used for debugging.
#[derive(Clone, Copy, Debug, Default)]
pub struct Statistics {
	/// Total amount of allocations in this session.
	pub allocations: u64,
	/// Total amount of deallocations in this session.
	pub deallocations: u64,
	/// Total amount of blocks allocated in this session.
	pub allocated_blocks: u64,
	/// Total amount of blocks deallocated in this session.
	pub deallocated_blocks: u64,
}

impl Default for Allocator {
	fn default() -> Self {
		Self {
			// Pretend everything is allocated for `load` and `assert_alloc`
			alloc_map: [(0..u64::MAX)].into_iter().collect(),
			free_map: Default::default(),
			dirty_map: Default::default(),
			stack: Default::default(),
			statistics: Default::default(),
			#[cfg(feature = "debug-trace-alloc")]
			debug_alloc_traces: Default::default(),
			#[cfg(feature = "debug-trace-alloc")]
			debug_dealloc_traces: Default::default(),
		}
	}
}

impl Allocator {
	pub async fn load<D, R>(store: &Store<D, R>) -> Result<Self, Error<D>>
	where
		D: Dev,
		R: Resource,
	{
		trace!("load");

		let mut alloc_map = RangeSet::new();
		let mut stack = Vec::new();

		// Iterate stack from top to bottom
		let mut record = store.devices.allocation_log.get();
		while record.length > 0 {
			// Append to stack for later ops
			stack.push(record);

			// Get record data
			let data = store.read(&record).await?;
			let lba = u64::from(record.lba);
			let size = u64::try_from(data.len()).unwrap();
			let end = usize::try_from(size).unwrap();

			// Add record itself
			let record_blocks =
				u64::try_from(store.calc_block_count(record.length.into())).unwrap();
			alloc_map.insert(lba..lba + record_blocks);

			// Add entries
			let entry_mask = mem::size_of::<Entry>() - 1;
			for i in (mem::size_of::<Record>()..(end + entry_mask) & !entry_mask).step_by(16) {
				// Get entry
				let max_len = (end - i).min(mem::size_of::<Entry>());
				let mut entry = Entry::default();
				entry.as_mut()[..max_len].copy_from_slice(&data.get()[i..i + max_len]);

				let (mut start, end) = (u64::from(entry.lba), u64::from(entry.lba + entry.size));

				// Xor with overlapping ranges.
				while start < end {
					// Get end of first next range.
					let range = alloc_map.gaps(&(start..end)).next().unwrap_or(end..end);
					// Determine mid point.
					let mid = range.end.min(end);
					// The first range we got indicates free memory, so current state is:
					//
					//   |AAAAAAAAAA|DDDDDDDDD|
					//   ^          ^         ^
					// start       mid       end
					//
					// Mark the first half as deallocated and the second half as allocated.
					if start < mid {
						alloc_map.insert(start..mid);
					}
					if mid < end {
						alloc_map.remove(mid..end);
					}
					// Begin next range.
					start = end;
				}
			}

			// Next record
			record = util::get_record(data.get(), 0).unwrap_or_default();
		}

		trace!("==>  {:?}", &alloc_map);

		Ok(Self {
			alloc_map,
			free_map: Default::default(),
			dirty_map: Default::default(),
			stack,
			statistics: Default::default(),
			#[cfg(feature = "debug-trace-alloc")]
			debug_alloc_traces: Default::default(),
			#[cfg(feature = "debug-trace-alloc")]
			debug_dealloc_traces: Default::default(),
		})
	}

	pub fn alloc(&mut self, blocks: u64, block_count: u64) -> Option<u64> {
		if blocks == 0 {
			return Some(0);
		}
		trace!("alloc {}", blocks);
		//todo!();
		for r in self.alloc_map.gaps(&(0..block_count)) {
			if r.end - r.start >= blocks {
				self.alloc_map.insert(r.start..r.start + blocks);
				self.dirty_map.insert(r.start..r.start + blocks);
				self.statistics.allocations += 1;
				self.statistics.allocated_blocks += blocks;
				#[cfg(feature = "debug-trace-alloc")]
				{
					for i in r.clone() {
						self.debug_dealloc_traces.remove(&i);
					}
					let r = self
						.debug_alloc_traces
						.insert(r.start, std::backtrace::Backtrace::capture());
					assert!(r.is_none(), "double alloc");
				}
				trace!("--> {}", r.start);
				return Some(r.start);
			}
		}
		trace!("--> N/A");
		None
	}

	pub fn free(&mut self, start: u64, blocks: u64) {
		// TODO RangeSet panics if blocks == 0.
		// It would make more sense if it just ignored the range.
		if blocks == 0 {
			return;
		}
		trace!("free {}, len {}", start, blocks);

		#[cfg(feature = "debug-trace-alloc")]
		{
			self.debug_alloc_traces.remove(&start);
			let r = self
				.debug_dealloc_traces
				.insert(start, std::backtrace::Backtrace::capture());
			if let Some(r) = r {
				panic!("double free! Previous deallocation at:\n{:#}", r);
			}
		}

		// FIXME really stupid
		for i in start..start + blocks {
			debug_assert!(self.alloc_map.contains(&i), "double free (lba: {})", i);
			debug_assert!(!self.free_map.contains(&i), "double free (lba: {})", i);
			if !cfg!(feature = "never-overwrite-in-transaction") && self.dirty_map.contains(&i) {
				self.dirty_map.remove(i..i + 1);
				self.alloc_map.remove(i..i + 1);
			} else {
				self.free_map.insert(i..i + 1);
			}
		}
		self.statistics.deallocations += 1;
		self.statistics.deallocated_blocks += blocks;
	}

	/// Ensure all blocks in a range are allocated.
	///
	/// Used to detect use-after-frees.
	#[cfg(debug_assertions)]
	pub fn assert_alloc(&self, start: u64, blocks: u64) {
		#[cfg(feature = "debug-trace-alloc")]
		{
			if let Some(trace) = self.debug_dealloc_traces.get(&start) {
				panic!("use-after-free. Freed at\n{}", trace);
			}
			if !self.debug_alloc_traces.contains_key(&start) {
				panic!("use of unallocated memory");
			}
		}
		// FIXME really stupid
		for i in start..start + blocks {
			debug_assert!(self.alloc_map.contains(&i), "use-after-free (lba: {})", i);
			debug_assert!(!self.free_map.contains(&i), "use-after-free (lba: {})", i);
		}
	}

	/// Save the allocator state.
	pub async fn save<D, R>(&mut self, store: &Store<D, R>) -> Result<(), Error<D>>
	where
		D: Dev,
		R: Resource,
	{
		{
			trace!("save");
		}
		{
			trace!("  alloc  {:?}", &self.alloc_map);
		}
		{
			trace!("  dirty  {:?}", &self.dirty_map);
		}
		{
			trace!("  free   {:?}", &self.free_map);
		}

		// Update map
		// TODO it would be nice if we could avoid a Clone.
		let mut alloc_map = self.alloc_map.clone();
		for r in self.free_map.iter() {
			alloc_map.remove(r.clone());
		}

		trace!("    -->  {:?}", &alloc_map);

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
		let mut buf = store.devices.resource.alloc();
		while iter.peek().is_some() {
			// Reference previous record
			buf.resize(0, 0);
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
				buf.len() > 0,
				"buffer should have at least one log entry with non-zero size"
			);
			let len = store.round_block_size(buf.len().try_into().unwrap());

			// FIXME we should poll writes while waiting for an alloc,
			// as it is possible all memory is used up by the current writes.
			let mut b = store.devices.alloc(len).await?;

			prev = Record::pack(
				buf.get(),
				b.get_mut(),
				store.compression(),
				store.block_size(),
			);
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
			writes.push(store.devices.write(lba, b, Set256::set_all()));
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

		trace!("    ==>  {:?}", &self.alloc_map);

		Ok(())
	}
}
