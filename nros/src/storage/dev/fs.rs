use {
	super::{Allocator, Buf, Dev},
	crate::BlockSize,
	alloc::sync::Arc,
	core::{cell::RefCell, future},
	std::{
		fs::File,
		io::{Read, Seek, SeekFrom, Write},
	},
};

/// A pseudo-device wrapping a file.
#[derive(Debug)]
pub struct FileDev {
	file: RefCell<File>,
	block_count: u64,
}

#[derive(Debug)]
pub enum FileDevError {
	OutOfRange,
	BlockSizeMismatch,
	Io(std::io::Error),
}

impl FileDev {
	/// Wrap a new file.
	pub fn new(mut file: File) -> Self {
		let len = file.seek(SeekFrom::End(0)).expect("failed to seek");
		Self { file: file.into(), block_count: len / 512 }
	}

	fn seek(&self, lba: u64, len: usize) -> Result<(), FileDevError> {
		let end = lba.saturating_add((len / 512).try_into().unwrap());
		if len % 512 != 0 {
			Err(FileDevError::BlockSizeMismatch)
		} else if end > self.block_count {
			Err(FileDevError::OutOfRange)
		} else {
			self.file
				.borrow_mut()
				.seek(SeekFrom::Start(lba * 512))
				.map(|_| ())
				.map_err(FileDevError::Io)
		}
	}
}

impl Dev for FileDev {
	type Error = FileDevError;
	type ReadTask<'a> = future::Ready<Result<Arc<Vec<u8>>, Self::Error>>
	where
		Self: 'a;
	type WriteTask<'a> = future::Ready<Result<(), Self::Error>>
	where
		Self: 'a;
	type FenceTask<'a> = future::Ready<Result<(), Self::Error>>
	where
		Self: 'a;
	type Allocator = FileAllocator;

	fn block_count(&self) -> u64 {
		self.block_count
	}

	fn block_size(&self) -> BlockSize {
		BlockSize::B512
	}

	fn read<'a>(&self, lba: u64, len: usize) -> Self::ReadTask<'_> {
		future::ready(self.seek(lba, len).and_then(|()| {
			let mut buf = vec![0; len];
			self.file
				.borrow_mut()
				.read_exact(&mut buf)
				.map(|()| buf.into())
				.map_err(FileDevError::Io)
		}))
	}

	fn write(&self, lba: u64, buf: <Self::Allocator as Allocator>::Buf) -> Self::WriteTask<'_> {
		future::ready(self.seek(lba, buf.len()).and_then(|()| {
			self.file
				.borrow_mut()
				.write_all(&buf)
				.map_err(FileDevError::Io)
		}))
	}

	fn fence(&self) -> Self::FenceTask<'_> {
		future::ready(self.file.borrow_mut().sync_all().map_err(FileDevError::Io))
	}

	fn allocator(&self) -> &Self::Allocator {
		&FileAllocator
	}
}

/// Allocator returning heap buffers.
/// For use with [`FileDev`]
pub struct FileAllocator;

impl Allocator for FileAllocator {
	type Error = FileDevError;
	type AllocTask<'a> = future::Ready<Result<Self::Buf, Self::Error>>
	where
		Self: 'a;
	type Buf = Arc<Vec<u8>>;

	fn alloc(&self, size: usize) -> Self::AllocTask<'_> {
		future::ready(Ok(vec![0; size].into()))
	}
}
