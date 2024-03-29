use {
	super::{Dev, Set256, Store},
	crate::{
		data::record::{self, RecordRef},
		resource::Buf,
		util, Error, Resource,
	},
	core::mem,
	endian::u64le,
	futures_util::stream::{FuturesUnordered, TryStreamExt},
	rangemap::RangeSet,
};

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
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
	stack: Vec<RecordRef>,
	/// Allocator statistics.
	///
	/// Used for debugging.
	pub(super) statistics: Statistics,
	#[cfg(feature = "debug-trace-alloc")]
	debug_alloc_traces: alloc::collections::BTreeMap<u64, std::backtrace::Backtrace>,
	#[cfg(feature = "debug-trace-alloc")]
	debug_dealloc_traces: alloc::collections::BTreeMap<u64, std::backtrace::Backtrace>,
	#[cfg(feature = "debug-trace-alloc")]
	debug_disable: bool,
}

/// Statistics for this session.
///
/// Used for debugging.
#[derive(Clone, Copy, Debug, Default)]
pub struct Statistics {
	/// Total amount of blocks available
	pub total_blocks: u64,
	/// Amount of blocks allocated.
	pub used_blocks: u64,
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
			// Whether to skip debug_alloc traces.
			// Necessary for Allocator::load et al.
			#[cfg(feature = "debug-trace-alloc")]
			debug_disable: true,
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

		// Collect stack first to speed up building up rangemap
		let mut record = store.devices.allocation_log_head.get();
		while record.blocks() > 0 {
			stack.push(record);
			let data = store.read(record).await?;
			util::read(0, record.as_mut(), data.get());
		}

		// Iterate log from start
		for &rec in stack.iter().rev() {
			let data = store.read(rec).await?;

			// Add record itself
			alloc_map.insert(rec.lba()..rec.lba() + u64::from(rec.blocks()));

			let (_, data) = data.get().split_at(8);

			// Add entries
			for entry_raw in data.chunks(16) {
				// Get entry
				let mut entry = Entry::default();
				util::read(0, entry.as_mut(), entry_raw);

				let (start, end) = (u64::from(entry.lba), u64::from(entry.lba + entry.size));

				// Xor with overlapping range.
				if alloc_map.contains(&start) {
					alloc_map.remove(start..end);
				} else {
					alloc_map.insert(start..end);
				}
			}
		}

		trace!("==>  {:?}", &alloc_map);

		let used_blocks = alloc_map.iter().fold(0, |x, r| r.end - r.start + x);

		Ok(Self {
			#[cfg(feature = "debug-trace-alloc")]
			debug_alloc_traces: Default::default(),
			#[cfg(feature = "debug-trace-alloc")]
			debug_dealloc_traces: Default::default(),
			#[cfg(feature = "debug-trace-alloc")]
			debug_disable: !stack.is_empty(),

			alloc_map,
			free_map: Default::default(),
			dirty_map: Default::default(),
			stack,
			statistics: Statistics {
				total_blocks: 0, // TODO
				used_blocks,
				..Default::default()
			},
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
				self.statistics.used_blocks += blocks;
				#[cfg(feature = "debug-trace-alloc")]
				{
					for i in r.clone() {
						self.debug_dealloc_traces.remove(&i);
					}
					let r = self
						.debug_alloc_traces
						.insert(r.start, std::backtrace::Backtrace::capture());
					assert!(r.is_none(), "double alloc\n{:#?}", r);
				}
				trace!(info "{}", r.start);
				return Some(r.start);
			}
		}
		trace!(info "N/A");
		None
	}

	pub fn free(&mut self, start: u64, blocks: u64) {
		// TODO RangeSet panics if blocks == 0.
		// It would make more sense if it just ignored the range.
		if blocks == 0 {
			return;
		}
		trace!("free {}+{}", start, blocks);

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
			if !cfg!(feature = "never-overwrite") && self.dirty_map.contains(&i) {
				self.dirty_map.remove(i..i + 1);
				self.alloc_map.remove(i..i + 1);
			} else {
				self.free_map.insert(i..i + 1);
			}
		}
		self.statistics.deallocations += 1;
		self.statistics.deallocated_blocks += blocks;
		self.statistics.used_blocks -= blocks;
	}

	/// Ensure all blocks in a range are allocated.
	///
	/// Used to detect use-after-frees.
	#[cfg(debug_assertions)]
	pub fn assert_alloc(&self, start: u64, blocks: u64) {
		#[cfg(feature = "debug-trace-alloc")]
		if !self.debug_disable {
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
	///
	/// Returns all freed blocks, which can be discarded.
	pub async fn save<D, R>(&mut self, store: &Store<D, R>) -> Result<RangeSet<u64>, Error<D>>
	where
		D: Dev,
		R: Resource,
	{
		trace!("save");
		trace!(info "alloc  {:?}", &self.alloc_map);
		trace!(info "dirty  {:?}", &self.dirty_map);
		trace!(info "free   {:?}", &self.free_map);

		// Update map
		// TODO it would be nice if we could avoid a Clone.
		let mut alloc_map = self.alloc_map.clone();
		for r in self.free_map.iter() {
			alloc_map.remove(r.clone());
		}

		trace!(info "{:?}", &alloc_map);

		// Save map
		// TODO avoid writing the entire log every time.

		// Deallocate all stack records of current log.
		for record in self.stack.drain(..) {
			let lba = record.lba();
			let blocks = record.blocks();
			alloc_map.remove(lba..lba + u64::try_from(blocks).unwrap());
			#[cfg(feature = "debug-trace-alloc")]
			self.debug_alloc_traces.remove(&lba);
		}

		let mut iter = alloc_map.iter().peekable();
		let rec_size = 1 << store.max_rec_size().to_raw();
		let entries_per_record = (rec_size - mem::size_of::<RecordRef>()) / mem::size_of::<Entry>();

		// Perform writes concurrently to speed things up a bit.
		let writes = FuturesUnordered::new();

		let mut prev = RecordRef::default();
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
			let len = store
				.block_size()
				.round_up(usize::from(record::HEADER_LEN) + buf.len());

			// FIXME we should poll writes while waiting for an alloc,
			// as it is possible all memory is used up by the current writes.
			let mut b = store.devices.alloc(len).await?;

			let blocks = record::pack(
				buf.get(),
				b.get_mut(),
				store.compression(),
				store.block_size(),
				store.devices.cipher(),
				&store.devices.gen_nonce(),
			);
			b.shrink(usize::from(blocks) << store.block_size().to_raw());

			// Store record
			let lba = self
				.alloc(blocks.into(), store.devices.block_count())
				.ok_or(Error::NotEnoughSpace)?;
			writes.push(store.devices.write(lba, b, Set256::set_all()));

			prev = RecordRef::new(lba, blocks);
			self.stack.push(prev);
		}

		// Finish writes
		writes.try_collect().await?;
		store.devices.allocation_log_head.set(prev);

		// Update alloc_map with *implicitly* recorded allocations for stack records.
		for record in self.stack.iter() {
			let lba = record.lba();
			let blocks = record.blocks();
			alloc_map.insert(lba..lba + u64::from(blocks));
		}

		// Clear free & dirty ranges.
		self.alloc_map = alloc_map;
		let free_map = mem::take(&mut self.free_map);
		self.dirty_map = Default::default();

		trace!(final "{:?}", &self.alloc_map);

		Ok(free_map)
	}
}
