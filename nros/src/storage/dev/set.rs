use super::{Allocator, Dev, Buf};
use crate::{RecordTree, BlockSize, header::Header, Error};
use futures_util::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use core::future;

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
	/// The total amount of blocks covered in each chain.
	block_count: u64,
}

impl<D: Dev> DevSet<D> {
	/// Save headers to the head or tail all devices.
	///
	/// Headers **must** always be saved at the tail before the head.
	pub async fn save_headers(&self) -> Result<(), D::Error> {
		// Allocate buffers for headers & write to tails.
		let block_size = self.block_size();
		let fut = self.devices.iter()
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
		let fut = fut
			.try_collect::<Vec<_>>()
			.await?;

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
	pub async fn read(&self, lba: u64, count: usize) -> Result<<D::Allocator as Allocator>::Buf<'_>, Error<D>> {
		todo!()
	}

	/// Write a range of blocks.
	pub async fn write(&self, lba: u64, data: <D::Allocator as Allocator>::Buf<'_>) -> Result<(), Error<D>> {
		todo!()
	}

	/// Flush & ensure all writes have completed.
	pub async fn fence(&self) -> Result<(), Error<D>> {
		todo!()
	}

	/// Allocate memory for writing.
	pub async fn alloc(&self, len: usize) -> Result<<D::Allocator as Allocator>::Buf<'_>, Error<D>> {
		self.devices[0][0].dev.allocator().alloc(len).await.map_err(Error::Dev)
	}

	/// Get a generic header.
	pub async fn header(&self) -> GenericHeader {
		todo!()
	}

	/// The block size used by this device set.
	///
	/// This may differ from the underlying devices.
	/// The device set will automatically compensate for this.
	pub fn block_size(&self) -> BlockSize {
		self.block_size
	}

	/// The total amount of blocks addressable by this device set.
	pub fn block_count(&self) -> u64 {
		self.block_count
	}
}

/// Generic NROS header.
/// That is, a header that has no device-specific information.
pub struct GenericHeader {
	pub version: u32,
	pub block_length_p2: u8,
	pub max_record_length_p2: u8,
	pub compression: u8,
	pub mirror_count: u8,
	pub uid: u64,

	pub total_block_count: u64,

	pub object_list: RecordTree,

	pub allocation_log_lba: u64,
	pub allocation_log_length: u64,

	pub generation: u32,
}

/// Create a header for writing to a device.
async fn create_header<D: Dev>(block_size: BlockSize, dev: &D, start: u64, len: u64) -> Result<<D::Allocator as Allocator>::Buf<'_>, D::Error> {
	let mut header = Header {
		..todo!()
	};
	header.update_xxh3();

	let mut buf = dev.allocator().alloc(1 << block_size.to_raw()).await?;
	//let buf = buf.get_mut().await?; // FIXME wtf?
	let b = buf.get_mut().await.unwrap_or_else(|_| todo!());
	b[..header.as_ref().len()].copy_from_slice(header.as_ref());
	Ok(buf)
}

/// Save a single header to a device.
async fn save_header<D: Dev>(tail: bool, dev: &D, len: u64, header: <D::Allocator as Allocator>::Buf<'_>) -> Result<(), D::Error> {
	let lba = if tail {
		0
	} else {
		len
	};

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
