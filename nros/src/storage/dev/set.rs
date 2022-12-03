use {
	super::{Allocator, Buf, Dev},
	crate::{header::Header, BlockSize, Compression, Error, MaxRecordSize, Record},
	core::future,
	futures_util::stream::{FuturesUnordered, StreamExt, TryStreamExt},
};

/// A single device with some extra information.
#[derive(Debug)]
struct Node<D> {
	/// The device itself.
	dev: D,
	/// The offset of the blocks of this device in the chain.
	block_offset: u64,
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

	pub allocation_log_lba: u64,
	pub allocation_log_length: u64,
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
					.map(|dev| Node { dev, block_offset: 0 })
					.collect::<Box<_>>()
			})
			.collect::<Box<_>>();

		assert!(devices.iter().all(|c| !c.is_empty()), "empty chain");
		assert!(
			devices
				.iter()
				.flat_map(|c| c.iter())
				.all(|d| d.dev.block_count() > 0),
			"device with no blocks"
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
			.map(|c| c.iter().map(|d| d.dev.block_count()).sum())
			.min()
			.expect("no chains");

		// Assign block offsets to devices in chains and write headers.
		for chain in devices.iter_mut() {
			let mut block_offset = 0;
			for node in chain.iter_mut() {
				node.block_offset = block_offset;
				block_offset += node.dev.block_count();
			}
		}

		Ok(Self {
			devices,
			block_size,
			max_record_size,
			compression,
			block_count,
			allocation_log_lba: 0,
			allocation_log_length: 0,
		})
	}

	/// Load an existing device set.
	pub async fn load<I>(devices: I) -> Result<Self, Error<D>> {
		todo!()
	}

	/// Save headers to the head or tail all devices.
	///
	/// Headers **must** always be saved at the tail before the head.
	pub async fn save_headers(&self) -> Result<(), D::Error> {
		// Allocate buffers for headers & write to tails.
		let block_size = self.block_size();
		let fut = self
			.devices
			.iter()
			.map(|chain| {
				let mut offset = self.block_count;
				chain.iter().map(move |node| {
					let len = offset - node.block_offset;
					offset = node.block_offset;
					// TODO this is ugly as hell.
					create_and_save_header_tail(block_size, &node.dev, 0, len)
				})
			})
			.flatten()
			.collect::<FuturesUnordered<_>>();

		// Wait for tail futures to finish & collect them.
		let fut = fut.try_collect::<Vec<_>>().await?;

		// Now save heads.
		let fut: FuturesUnordered<_> = fut
			.into_iter()
			.map(|(dev, buf, len)| save_header(false, dev, len, buf))
			.collect::<FuturesUnordered<_>>();

		// Wait for head futures to finish
		fut.try_for_each(|f| future::ready(Ok(f))).await
	}

	/// Read a range of blocks.
	///
	/// A chain blacklist can be used in case corrupt data was returned.
	pub async fn read(&self, lba: u64, count: usize) -> Result<SetBuf<D>, Error<D>> {
		todo!()
	}

	/// Write a range of blocks.
	pub async fn write(&self, lba: u64, data: SetBuf<'_, D>) -> Result<(), Error<D>> {
		todo!()
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
		todo!()
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

/// Create a header for writing to a device.
async fn create_header<D: Dev>(
	block_size: BlockSize,
	dev: &D,
	start: u64,
	len: u64,
) -> Result<<D::Allocator as Allocator>::Buf<'_>, D::Error> {
	let mut header = Header { ..todo!() };
	header.update_xxh3();

	let mut buf = dev.allocator().alloc(1 << block_size.to_raw()).await?;
	buf.get_mut()[..header.as_ref().len()].copy_from_slice(header.as_ref());
	Ok(buf)
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

/// Create header & save to tail of device
async fn create_and_save_header_tail<D: Dev>(
	block_size: BlockSize,
	dev: &D,
	start: u64,
	len: u64,
) -> Result<(&D, <D::Allocator as Allocator>::Buf<'_>, u64), D::Error> {
	let buf = create_header(block_size, dev, 0, 0).await?;
	let b = buf.deep_clone().await.map_err(|_| todo!())?; // FIXME
	save_header(true, dev, len, b).await?;
	Ok((dev, buf, len))
}
