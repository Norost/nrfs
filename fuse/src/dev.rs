use std::os::fd::AsRawFd;

use {
	nrfs::dev::Allocator,
	std::{
		fs::File,
		io::Seek,
		os::unix::fs::{FileExt, FileTypeExt},
		sync::Arc,
	},
};

#[derive(Debug)]
pub struct Dev {
	file: Arc<File>,
	block_count: u64,
	#[cfg(target_os = "linux")]
	can_discard: bool,
}

impl Dev {
	pub fn new(mut file: File) -> Self {
		let block_count = file.stream_position().unwrap();
		Self {
			#[cfg(target_os = "linux")]
			can_discard: file.metadata().unwrap().file_type().is_block_device(),
			file: file.into(),
			block_count,
		}
	}
}

impl nrfs::Dev for Dev {
	type Error = std::io::Error;
	type Allocator = Alloc;
	type ReadTask<'a> = blocking::Task<Result<Arc<Vec<u8>>, Self::Error>>;
	type WriteTask<'a> = blocking::Task<Result<(), Self::Error>>;
	type FenceTask<'a> = blocking::Task<Result<(), Self::Error>>;
	#[cfg(target_os = "linux")]
	type DiscardTask<'a> = blocking::Task<Result<(), Self::Error>>;
	#[cfg(not(target_os = "linux"))]
	type DiscardTask<'a> = std::future::Ready<Result<(), Self::Error>>;

	fn read(&self, lba: u64, len: usize) -> Self::ReadTask<'_> {
		let file = self.file.clone();
		blocking::unblock(move || {
			let mut buf = vec![0; len];
			file.read_exact_at(&mut buf, lba << 9)?;
			Ok(buf.into())
		})
	}

	fn write(&self, lba: u64, data: Arc<Vec<u8>>) -> Self::WriteTask<'_> {
		let file = self.file.clone();
		blocking::unblock(move || file.write_all_at(&data, lba << 9))
	}

	fn fence(&self) -> Self::FenceTask<'_> {
		let file = self.file.clone();
		blocking::unblock(move || file.sync_data())
	}

	fn discard(&self, _lba: u64, _blocks: u64) -> Self::DiscardTask<'_> {
		#[cfg(target_os = "linux")]
		{
			// linux/fs.h
			const BLKDISCARD: u64 = ((0) << (((0 + 8) + 8) + 14))
				| ((0x12) << (0 + 8))
				| ((119) << 0) | ((0) << ((0 + 8) + 8));
			let fd = self.file.as_raw_fd();
			let can_discard = self.can_discard;
			blocking::unblock(move || {
				if !can_discard {
					return Ok(());
				}
				// util-linux/sys-utils/blkdiscard.c
				let start = _lba << 9;
				let end = (_lba + _blocks) << 9;
				let mut range = [start, end - start];
				while range[0] < end {
					// FIXME wtf is this?
					if range[0] + range[1] > end {
						range[1] = end - range[0];
					}
					let res = unsafe { libc::ioctl(fd, BLKDISCARD, &mut range) };
					if res < 0 {
						// FIXME we should check /sys/dev/block/X/queue/discard_max_bytes
						//return Err(std::io::Error::from_raw_os_error(res));
						return Ok(());
					}
					range[0] += range[1];
				}
				Ok(())
			})
		}
		#[cfg(not(target_os = "linux"))]
		std::future::ready(Ok(()))
	}

	fn allocator(&self) -> &Self::Allocator {
		&Alloc
	}

	fn block_count(&self) -> u64 {
		self.block_count
	}

	fn block_size(&self) -> nrfs::BlockSize {
		nrfs::BlockSize::B512
	}
}

pub struct Alloc;

impl Allocator for Alloc {
	type Buf = Arc<Vec<u8>>;
	type Error = std::io::Error;
	type AllocTask<'a> = std::future::Ready<Result<Self::Buf, Self::Error>>
	where
		Self: 'a;

	fn alloc(&self, len: usize) -> Self::AllocTask<'_> {
		std::future::ready(Ok(Arc::new(vec![0; len])))
	}
}
