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
			allocation_log: Default::default(),
			object_list: Default::default(),
		})
	}

	/// Load an existing device set.
	///
	/// `read_only` prevents fixing any headers that may be broken.
	pub async fn load(devices: Vec<D>, read_only: bool) -> Result<Self, Error<D>> {
		// We're looking to retrieve two types of data from the devices:
		//
		// 1. Global info that is shared among all devices.
		// 2. Per-device info that we need to retrieve per device.
		//
		// For global info any valid header from any device will do.
		// For per-device info we need any valid header per device.

		// Collect both start & end headers.
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

		let has_broken_headers = Cell::new(false);
		let mut uid = None;
		let mut uid_mismatch = false;

		// 1. Global info that is shared among all devices.
		//
		// Prefer start headers as those are more likely to be up to date than end headers.
		let header = headers
			.iter()
			.map(|(h, _)| h)
			.chain(headers.iter().map(|(_, h)| h))
			.flat_map(|buf| {
				has_broken_headers.update(|x| x | buf.as_ref().is_err());
				buf.as_ref().ok()
			})
			.find_map(|buf| {
				let mut header = Header::default();
				header
					.as_mut()
					.copy_from_slice(&buf.get()[..mem::size_of::<Header>()]);

				let valid = header.verify_xxh3();
				has_broken_headers.update(|x| x | !valid);

				uid_mismatch |= *uid.get_or_insert(header.uid) != header.uid;

				valid.then_some(header)
			})
			.unwrap_or_else(|| todo!("no valid headers"));

		if uid_mismatch {
			todo!("return error when dealing with mismatched UIDs");
		}

		// Build mirrors
		let mut mirrors = (0..u8::from(header.mirror_count))
			.map(|_| Vec::new())
			.collect::<Vec<_>>();
		for (i, bufs) in headers.iter().enumerate() {
			// Use any valid header.
			let get = |buf: &[u8]| {
				let mut header = Header::default();
				header
					.as_mut()
					.copy_from_slice(&buf[..mem::size_of::<Header>()]);
				header
			};
			let header = match bufs {
				(Ok(buf), _) if get(buf.get()).verify_xxh3() => get(buf.get()),
				(_, Ok(buf)) if get(buf.get()).verify_xxh3() => get(buf.get()),
				(Ok(_), _) | (_, Ok(_)) => todo!("no header with valid xxh3"),
				(Err(_), Err(_)) => todo!("one device failed, continue with other chains"),
			};

			// Add to mirror.
			mirrors
				.get_mut(usize::from(u8::from(header.mirror_index)))
				.expect("todo: invalid mirror index")
				.push((
					i,
					u64::from(header.lba_offset),
					u64::from(header.block_count),
				))
		}

		drop(headers);

		// Sort each device and check for gaps.
		for chain in mirrors.iter_mut() {
			chain.sort_unstable_by_key(|(_, lba_offset, _)| *lba_offset);
			// TODO check gaps
		}

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

		let s = Self {
			devices,

			block_size: BlockSize::from_raw(header.block_length_p2).expect("todo"),
			max_record_size: MaxRecordSize::from_raw(header.max_record_length_p2).expect("todo"),
			compression: Compression::from_raw(header.compression).expect("todo"),
			uid: header.uid,
			block_count: header.total_block_count.into(),

			object_list: header.object_list.into(),
			allocation_log: header.allocation_log.into(),
		};

		// If any headers are broken, fix them now.
		if !read_only && has_broken_headers.get() {
			s.save_headers().await?;
		}

		Ok(s)
	}

	/// Save headers to the start and end of all devices.
	///
	/// This performs a fence before writing to the start
	/// and another fence before writing to the end.
	pub async fn save_headers(&self) -> Result<(), Error<D>> {
		// First ensure all data is flushed.
		self.devices
			.iter()
			.flat_map(|chain| chain.iter())
			.map(|node| node.dev.fence())
			.collect::<FuturesUnordered<_>>()
			.try_for_each(|()| future::ready(Ok(())))
			.await
			.map_err(Error::Dev)?;

		/// Save a single header to a device.
		async fn save_header<D: Dev>(
			tail: bool,
			node: &Node<D>,
			header: <D::Allocator as Allocator>::Buf<'_>,
		) -> Result<(), D::Error> {
			let lba = if tail { 0 } else { 1 + node.block_count };
			node.dev.write(lba, header);
			node.dev.fence().await
		}

		// Allocate buffers for headers & write to start headers.
		let fut = self
			.devices
			.iter()
			.enumerate()
			.map(|(i, chain)| {
				chain.iter().map(move |node| async move {
					let buf = self.create_header(i.try_into().unwrap(), node).await?;
					let b = buf.clone();
					save_header(false, node, b).await?;
					Ok((node, buf))
				})
			})
			.flatten()
			.collect::<FuturesUnordered<_>>();

		// Wait for futures to finish.
		let fut = fut.try_collect::<Vec<_>>().await.map_err(Error::Dev)?;

		// Now save to end.
		let fut: FuturesUnordered<_> = fut
			.into_iter()
			.map(|(node, buf)| save_header(true, node, buf))
			.collect::<FuturesUnordered<_>>();

		// Wait for futures to finish
		fut.try_for_each(|f| future::ready(Ok(f)))
			.await
			.map_err(Error::Dev)
	}

	/// Read a range of blocks.
	///
	/// A chain blacklist can be used in case corrupt data was returned.
	///
	/// The chain from which the data is read is returned.
	///
	/// If all available chains are blacklisted, `None` is returned.
	///
	/// # Note
	///
	/// If `size` isn't a multiple of the block size.
	pub async fn read(
		&self,
		lba: u64,
		size: usize,
		blacklist: &Set256,
	) -> Result<Option<(SetBuf<D>, u8)>, (Error<D>, u8)> {
		assert!(
			size % (1usize << self.block_size()) == 0,
			"data len isn't a multiple of block size"
		);

		let lba_end = lba.saturating_add(u64::try_from(size >> self.block_size()).unwrap());
		assert!(lba_end <= self.block_count, "read is out of bounds");

		// TODO balance loads
		for (i, chain) in self.devices.iter().enumerate() {
			let i = u8::try_from(i).unwrap();
			if blacklist.get(i) {
				continue;
			}

			// Do a binary search for the start device.
			let node_i = chain
				.binary_search_by_key(&lba, |node| node.block_offset)
				// if offset == lba, then we need that dev
				// if offset < lba, then we want the previous dev.
				.map_or_else(|i| i - 1, |i| i);
			let node = &chain[node_i];

			let node_block_end = node.block_offset + node.block_count;
			let node_lba = lba - node.block_offset + 1;

			// Check if the buffer range falls entirely within the device's range.
			// If not, split the buffer in two and perform two operations.
			return if lba_end <= node_block_end {
				// No splitting necessary - yay
				node.dev
					.read(node_lba, size)
					.await
					.map(|buf| Some((SetBuf(buf), i)))
					.map_err(|e| (Error::Dev(e), i))
			} else {
				// We need to split - aw
				// Figure out midpoint to split.
				let mid =
					size - usize::try_from(lba_end - node_block_end << self.block_size()).unwrap();
				// Allocate two buffers and read into each.
				let (buf_l, buf_r, mut buf) = futures_util::try_join!(
					node.dev.read(node_lba, mid),
					chain[node_i + 1].dev.read(1, size - mid),
					node.dev.allocator().alloc(size),
				)
				.map_err(|e| (Error::Dev(e), i))?;
				// Merge buffers.
				let b = buf.get_mut();
				b[..mid].copy_from_slice(buf_l.get());
				b[mid..].copy_from_slice(buf_r.get());
				Ok(Some((SetBuf(buf), i)))
			};
		}

		Ok(None)
	}

	/// Write a range of blocks.
	///
	/// The whitelist indicates which chains to write to.
	///
	/// # Panics
	///
	/// If the buffer size isn't a multiple of the block size.
	///
	/// If the buffer overlaps more than two devices.
	///
	/// If the write is be out of bounds.
	pub async fn write(
		&self,
		lba: u64,
		data: SetBuf<'_, D>,
		whitelist: Set256,
	) -> Result<(), Error<D>> {
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
			.enumerate()
			.filter_map(|(i, chain)| whitelist.get(i.try_into().unwrap()).then(|| chain))
			.map(|chain| {
				// Do a binary search for the start device.
				let node_i = chain
					.binary_search_by_key(&lba, |node| node.block_offset)
					// if offset == lba, then we need that dev
					// if offset < lba, then we want the previous dev.
					.map_or_else(|i| i - 1, |i| i);
				let node = &chain[node_i];

				// Check if the buffer range falls entirely within the device's range.
				// If not, split the buffer in two and perform two operations.
				let data = &data;
				async move {
					let node_lba_end = node.block_offset + node.block_count;
					let node_lba = lba - node.block_offset + 1;
					if lba_end <= node_lba_end {
						// No splitting necessary - yay
						node.dev.write(node_lba, data.0.clone()).await // +1 because headers
					} else {
						// We need to split - aw
						let d = data.get();
						// Figure out midpoint to split.
						let mid = d.len()
							- usize::try_from(lba_end - node_lba_end << self.block_size()).unwrap();
						// Allocate two buffers, copy halves to each and perform two writes.
						async fn f<D: Dev>(
							node: &Node<D>,
							lba: u64,
							data: &[u8],
						) -> Result<(), D::Error> {
							let mut buf = node.dev.allocator().alloc(data.len()).await?;
							buf.get_mut().copy_from_slice(data);
							node.dev.write(lba, buf).await
						}
						futures_util::try_join!(
							f(node, node_lba, &d[..mid]),
							f(&chain[node_i + 1], 1, &d[mid..])
						)
						.map(|((), ())| ())
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
		chain: u8,
		node: &'a Node<D>,
	) -> Result<<D::Allocator as Allocator>::Buf<'a>, D::Error> {
		let mut header = Header {
			compression: self.compression.to_raw(),
			block_length_p2: self.block_size.to_raw(),
			max_record_length_p2: self.max_record_size.to_raw(),
			mirror_count: self.devices.len().try_into().unwrap(),
			mirror_index: chain,

			uid: self.uid,

			lba_offset: node.block_offset.into(),
			block_count: node.block_count.into(),
			total_block_count: self.block_count.into(),

			object_list: self.object_list.get(),

			allocation_log: self.allocation_log.get(),

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

#[derive(Clone, Copy, Debug, Default)]
pub struct Set256(u128, u128);

impl Set256 {
	/// Create a new set with all bits set.
	pub fn set_all() -> Self {
		Self(u128::MAX, u128::MAX)
	}

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
