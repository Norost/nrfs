use super::{Allocator, Dev};
use crate::{RecordTree, BlockSize, header::Header};
use futures_util::stream::{FuturesUnordered, StreamExt};

/// A single device with some extra information.
struct Node<D> {
	/// The device itself.
	dev: D,
	/// The amount of blocks this device covers.
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
}

impl<D: Dev> DevSet<D> {
	/// Save headers to the head or tail all devices.
	///
	/// Headers **must** always be saved at the tail before the head.
	pub async fn save_headers(&mut self) -> Result<(), D::Error> {
		// Allocate buffers for headers & write to tails.
		let fut = self.devices.iter_mut()
			.flat_map(|chain| chain.iter_mut())
			.map(|(dev, len)| async move {
				let buf = self.create_header(dev, 0, 0).await;
				self.save_header(true, dev, len, buf.clone())?;
				Ok((dev, len, buf))
			})
			.collect::<FuturesUnordered<_>>();

		// Wait for tail futures to finish.
		let mut res = Vec::with_capacity(fut.len());
		while let Some(r) = fut.next().transpose()? {
			res.push(r);
		}

		// Now save heads
		let fut = fut.into_iter()
			.map(|(dev, len, buf)| self.save_header(false, dev, len, buf))
			.collect::<FuturesUnordered<_>>();

		fut.into_iter().try_for_each(|f| f)
	}

	/// Create a header for writing to a device.
	async fn create_header(&self, dev: &mut D, start: u64, len: u64) -> Result<<D::Allocator as Allocator>::Buf<'_>, D::Error> {
		let mut header = Header {

		};
		header.update_xxh3();

		let mut buf = dev.allocator().alloc(len << self.header.block_length_p2).await?;
		buf.get_mut()[..self.header.as_ref().len()].copy_from_slice(header.as_ref());
		Ok(buf)
	}

	/// Save a single header to a device.
	async fn save_header(&self, tail: bool, dev: &mut D, len: u64, header: <D::Allocator as Allocator>::Buf<'_>) -> Result<(), D::Error> {
		let lba = if tail {
			0
		} else {
			len
		};

		self.dev.write(lba, header);
		self.dev.fence().await
	}


	/// The amount of blocks this storage covers.
	pub fn len(&self) -> u64 {
		self.devices[0].iter().map(|(_, l)| *l).sum()
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
		self.devices[0].iter().map(|n| n.block_count).sum()
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
