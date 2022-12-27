use {
	super::{Allocator, Buf, Dev},
	crate::{header::Header, BlockSize, Compression, Error, MaxRecordSize, Record},
	core::{cell::Cell, future, mem},
	futures_util::stream::{FuturesUnordered, StreamExt, TryStreamExt},
};

/// A single device with some extra information.
#[derive(Debug)]
struct Node<D> {
	/// The device itself.
	dev: D,
	/// The offset of the blocks of this device in the chain.
	block_offset: u64,
	/// The amount of blocks covered by this device.
	block_count: u64,
}

/// Wrapper around a set of devices.
#[derive(Debug)]
pub struct DevSet<D: Dev> {
	/// The devices and their respective block counts.
	///
	/// It is an array of mirrors, which in turns represents a chain of devices.
	devices: Box<[Box<[Node<D>]>]>,
	/// The size of a block.
	block_size: BlockSize,
	/// The maximum size of a single record.
	max_record_size: MaxRecordSize,
	/// The default compression to use for records.
	compression: Compression,
	/// The total amount of blocks covered in each chain.
	block_count: u64,
	/// The unique identifier of this filesystem.
	uid: [u8; 16],
	/// A counter that indicates the total amount of updates to this filesystem.
	/// Wraps around (in a couple thousands of years).
	///
	/// Used to ensure data was properly flushed to all disks on each transaction.
	generation: Cell<u64>,

	pub allocation_log: Cell<Record>,
	pub object_list: Cell<Record>,
}

impl<D: Dev> DevSet<D> {
	/// Create a new device set.
	///
	/// # Note
	///
	/// This writes new headers to the devices.
	///
	/// # Panics
	///
	/// If `mirror` is empty.
	///
	/// If any chain is empty.
	///
	/// If any device has no blocks.
	pub async fn new<M, C>(
		mirrors: M,
		block_size: BlockSize,
		max_record_size: MaxRecordSize,
		compression: Compression,
	) -> Result<Self, Error<D>>
	where
		M: IntoIterator<Item = C>,
		C: IntoIterator<Item = D>,
	{
		// Collect devices into a convenient format.
		let mut devices = mirrors
			.into_iter()
			.map(|chain| {
				chain
					.into_iter()
					.map(|dev| Node { block_count: dev.block_count() - 2, dev, block_offset: 0 })
					.collect::<Box<_>>()
			})
			.collect::<Box<_>>();

		assert!(devices.iter().all(|c| !c.is_empty()), "empty chain");
		// Require that devices can contain a full-sized record to simplify
		// read & write operations as well as ensure some sanity in general.
		assert!(
			devices
				.iter()
				.flat_map(|c| c.iter())
				.all(|d| d.dev.block_count()
					>= 2 + (1 << (max_record_size.to_raw() - block_size.to_raw()))),
			"device cannot contain maximum size record & headers"
		);

		// FIXME support mismatched block sizes.
		assert!(
			devices
				.iter()
				.flat_map(|c| c.iter())
				.all(|d| d.dev.block_size().to_raw() == block_size.to_raw()),
			"todo: support mismatched block sizes"
		);

		// Determine length of smallest chain.
		let block_count = devices
			.iter()
			.map(|c| c.iter().map(|d| d.block_count).sum::<u64>())
			.min()
			.expect("no chains");

		// Assign block offsets to devices in chains and write headers.
		for chain in devices.iter_mut() {
			let mut block_offset = 0;
			for node in chain.iter_mut() {
				node.block_offset = block_offset;
				block_offset += node.block_count;
			}
		}

		Ok(Self {
			devices,
			block_size,
			max_record_size,
			compression,
			block_count,
			uid: *b" TODO TODO TODO ",
			generation: 0.into(),
			allocation_log: Default::default(),
			object_list: Default::default(),
		})
	}

	/// Load an existing device set.
	pub async fn load(devices: Vec<D>) -> Result<Self, Error<D>> {
		// Collect both head & tail headers.
		let headers = devices
			.iter()
			.map(|d| {
				let end = d.block_count() - 1;
				let block_size = 1usize << d.block_size();
				futures_util::future::join(d.read(0, block_size), d.read(end, block_size))
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		// Get global info from any valid header.
		let header = headers
			.iter()
			.flat_map(|(h, t)| [h, t])
			.filter_map(|h| h.as_ref().ok())
			.find_map(|buf| {
				let mut header = Header::default();
				header
					.as_mut()
					.copy_from_slice(&buf.get()[..mem::size_of::<Header>()]);
				header.verify_xxh3().then_some(header)
			})
			.unwrap_or_else(|| todo!("no valid headers"));

		// Check if all head or all tail headers are valid.
		//
		// TODO check xxh3 *and* generation.
		//let (all_head, all_tail) = headers.iter().map(|

		// Build mirrors
		let mut mirrors = (0..u8::from(header.mirror_count))
			.map(|_| Vec::new())
			.collect::<Vec<_>>();
		for (i, (head, tail)) in headers.iter().enumerate() {
			let head = head.as_ref().unwrap_or_else(|_| todo!());
			let mut header = Header::default();
			header
				.as_mut()
				.copy_from_slice(&head.get()[..mem::size_of::<Header>()]);
			assert!(header.verify_xxh3(), "todo");
			mirrors
				.get_mut(usize::from(u8::from(header.mirror_index)))
				.expect("todo")
				.push((
					i,
					u64::from(header.lba_offset),
					u64::from(header.block_count),
				))
		}
		// Sort each device and check for gaps.
		for chain in mirrors.iter_mut() {
			chain.sort_unstable_by_key(|(_, lba_offset, _)| *lba_offset);
			// TODO check gaps
		}

		drop(headers);

		// TODO avoid conversion to Vec<Option<_>>
		let mut devices = devices.into_iter().map(|d| Some(d)).collect::<Vec<_>>();

		// Collect in order.
		let devices = mirrors
			.into_iter()
			.map(|chain| {
				chain
					.into_iter()
					.map(|(i, block_offset, block_count)| Node {
						dev: devices[i].take().unwrap(),
						block_offset,
						block_count,
					})
					.collect()
			})
			.collect();

		Ok(Self {
			devices,

			block_size: BlockSize::from_raw(header.block_length_p2).expect("todo"),
			max_record_size: MaxRecordSize::from_raw(header.max_record_length_p2).expect("todo"),
			compression: Compression::from_raw(header.compression).expect("todo"),
			uid: header.uid,
			block_count: header.total_block_count.into(),

			object_list: header.object_list.into(),
			allocation_log: header.allocation_log.into(),
			generation: u64::from(header.generation).into(),
		})
	}

	/// Save headers to the head or tail all devices.
	///
	/// Headers **must** always be saved at the tail before the head.
	pub async fn save_headers(&self) -> Result<(), Error<D>> {
		// Allocate buffers for headers & write to tails.
		let fut = self
			.devices
			.iter()
			.map(|chain| {
				let mut offset = self.block_count;
				chain.iter().map(move |node| {
					let len = offset - node.block_offset;
					offset = node.block_offset;
					self.create_and_save_header_tail(node, len)
				})
			})
			.flatten()
			.collect::<FuturesUnordered<_>>();

		// Wait for tail futures to finish & collect them.
		let fut = fut.try_collect::<Vec<_>>().await.map_err(Error::Dev)?;

		// Now save heads.
		let fut: FuturesUnordered<_> = fut
			.into_iter()
			.map(|(dev, buf, len)| save_header(false, dev, len, buf))
			.collect::<FuturesUnordered<_>>();

		// Wait for head futures to finish
		fut.try_for_each(|f| future::ready(Ok(f)))
			.await
			.map_err(Error::Dev)
	}

	/// Read a range of blocks.
	///
	/// A chain blacklist can be used in case corrupt data was returned.
	///
	/// # Note
	///
	/// If `size` isn't a multiple of the block size.
	pub async fn read(
		&self,
		lba: u64,
		size: usize,
		blacklist: Set256,
	) -> Result<SetBuf<D>, Error<D>> {
		assert!(
			size % (1usize << self.block_size()) == 0,
			"data len isn't a multiple of block size"
		);

		let lba_end = lba.saturating_add(u64::try_from(size >> self.block_size()).unwrap());
		assert!(lba_end <= self.block_count, "read is out of bounds");

		// TODO balance loads
		for (i, chain) in self.devices.iter().enumerate() {
			if blacklist.get(i.try_into().unwrap()) {
				continue;
			}

			// Do a binary search for the start device.
			let node = chain
				.binary_search_by_key(&lba, |node| node.block_offset)
				// if offset == lba, then we need that dev
				// if offset < lba, then we want the previous dev.
				.map_or_else(|i| i - 1, |i| i);
			let node = &chain[node];

			let block_count = node.block_count;

			// Check if the buffer range falls entirely within the device's range.
			// If not, split the buffer in two and perform two operations.
			return if lba_end <= node.block_offset + block_count {
				// No splitting necessary - yay
				node.dev
					.read(lba - node.block_offset + 1, size)
					.await // +1 because header
					.map(SetBuf)
					.map_err(Error::Dev)
			} else {
				// We need to split - aw
				todo!()
			};
		}
		todo!("all chains failed. RIP")
	}

	/// Write a range of blocks.
	///
	/// # Panics
	///
	/// If the buffer size isn't a multiple of the block size.
	///
	/// If the write is be out of bounds.
	pub async fn write(&self, lba: u64, data: SetBuf<'_, D>) -> Result<(), Error<D>> {
		assert!(
			data.get().len() % (1usize << self.block_size()) == 0,
			"data len isn't a multiple of block size"
		);

		let lba_end =
			lba.saturating_add(u64::try_from(data.get().len() >> self.block_size()).unwrap());
		assert!(lba_end <= self.block_count, "write is out of bounds");

		// Write to all mirrors
		self.devices
			.iter()
			.map(|chain| {
				// Do a binary search for the start device.
				let node = chain
					.binary_search_by_key(&lba, |node| node.block_offset)
					// if offset == lba, then we need that dev
					// if offset < lba, then we want the previous dev.
					.map_or_else(|i| i - 1, |i| i);
				let node = &chain[node];

				let block_count = node.block_count;

				// Check if the buffer range falls entirely within the device's range.
				// If not, split the buffer in two and perform two operations.
				let data = &data;
				async move {
					if lba_end <= node.block_offset + block_count {
						// No splitting necessary - yay
						node.dev
							.write(lba - node.block_offset + 1, data.0.clone())
							.await // +1 because headers
					} else {
						// We need to split - aw
						todo!()
					}
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_for_each(|_| async { Ok(()) })
			.await
			.map_err(Error::Dev)
	}

	/// Create a header for writing to a device.
	async fn create_header<'a>(
		&self,
		node: &'a Node<D>,
	) -> Result<<D::Allocator as Allocator>::Buf<'a>, D::Error> {
		let mut header = Header {
			compression: self.compression.to_raw(),
			block_length_p2: self.block_size.to_raw(),
			max_record_length_p2: self.max_record_size.to_raw(),
			mirror_count: self.devices.len().try_into().unwrap(),

			uid: self.uid,

			lba_offset: node.block_offset.into(),
			block_count: node.block_count.into(),
			total_block_count: self.block_count.into(),

			object_list: self.object_list.get(),

			allocation_log: self.allocation_log.get(),

			generation: self.generation.get().into(),

			..Default::default()
		};
		header.update_xxh3();

		let mut buf = node
			.dev
			.allocator()
			.alloc(1 << self.block_size.to_raw())
			.await?;
		buf.get_mut()[..header.as_ref().len()].copy_from_slice(header.as_ref());
		Ok(buf)
	}

	/// Create header & save to tail of device
	async fn create_and_save_header_tail<'a>(
		&self,
		node: &'a Node<D>,
		len: u64,
	) -> Result<(&'a D, <D::Allocator as Allocator>::Buf<'a>, u64), D::Error> {
		let buf = self.create_header(node).await?;
		let b = buf.clone();
		save_header(true, &node.dev, len, b).await?;
		Ok((&node.dev, buf, len))
	}

	/// Flush & ensure all writes have completed.
	pub async fn fence(&self) -> Result<(), Error<D>> {
		todo!()
	}

	/// Allocate memory for writing.
	pub async fn alloc(&self, size: usize) -> Result<SetBuf<'_, D>, Error<D>> {
		self.devices[0][0]
			.dev
			.allocator()
			.alloc(size)
			.await
			.map(SetBuf)
			.map_err(Error::Dev)
	}

	/// Return the underlying devices.
	pub fn into_devices(self) -> Vec<D> {
		self.devices
			.into_vec()
			.into_iter()
			.flat_map(|c| c.into_vec())
			.map(|n| n.dev)
			.collect()
	}

	/// The block size used by this device set.
	///
	/// This may differ from the underlying devices.
	/// The device set will automatically compensate for this.
	pub fn block_size(&self) -> BlockSize {
		self.block_size
	}

	/// The maximum size of a single record.
	pub fn max_record_size(&self) -> MaxRecordSize {
		self.max_record_size
	}

	/// The default compression to use for records.
	pub fn compression(&self) -> Compression {
		self.compression
	}

	/// The total amount of blocks addressable by this device set.
	pub fn block_count(&self) -> u64 {
		self.block_count
	}
}

/// Buffer for use with [`DevSet`].
pub struct SetBuf<'a, D: Dev + 'a>(<D::Allocator as Allocator>::Buf<'a>);

impl<D: Dev> SetBuf<'_, D> {
	/// Get an immutable reference to the data.
	pub fn get(&self) -> &[u8] {
		self.0.get()
	}

	/// Get a mutable reference to the data.
	pub fn get_mut(&mut self) -> &mut [u8] {
		self.0.get_mut()
	}

	/// Deallocate blocks at the tail until the desired size is reached.
	///
	/// # Panics
	///
	/// If the new size is larger than the current size.
	pub fn shrink(&mut self, size: usize) {
		self.0.shrink(size);
	}
}

/// Generic NROS header.
/// That is, a header that has no device-specific information.
#[derive(Clone, Copy, Debug)]
pub struct GenericHeader {
	pub version: u32,
	pub mirror_count: u8,
	pub uid: u64,

	pub object_list: Record,

	pub allocation_log_lba: u64,
	pub allocation_log_length: u64,

	pub generation: u32,
}

/// Save a single header to a device.
async fn save_header<D: Dev>(
	tail: bool,
	dev: &D,
	len: u64,
	header: <D::Allocator as Allocator>::Buf<'_>,
) -> Result<(), D::Error> {
	let lba = if tail { 0 } else { len };

	dev.write(lba, header);
	dev.fence().await
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Set256(u128, u128);

impl Set256 {
	pub fn get(&self, bit: u8) -> bool {
		if bit < 0x80 {
			self.0 & 1 << bit > 0
		} else {
			let bit = bit & 0x7f;
			self.1 & 1 << bit > 0
		}
	}

	pub fn set(&mut self, bit: u8, value: bool) {
		if bit < 0x80 {
			self.0 &= !(1 << bit);
			self.0 |= u128::from(value) << bit;
		} else {
			let bit = bit & 0x7f;
			self.1 &= !(1 << bit);
			self.1 |= u128::from(value) << bit;
		}
	}
}
