use {
	super::{Allocator, Buf, Dev},
	crate::BlockSize,
	alloc::sync::Arc,
	core::{cell::RefCell, fmt, future},
	std::{
		fs::File,
		io::{Read, Seek, SeekFrom, Write},
	},
};

/// A pseudo-device wrapping a file.
pub struct FileDev {
	file: RefCell<File>,
	block_size: BlockSize,
	block_count: u64,
}

#[derive(Debug)]
pub enum FileDevError {
	OutOfRange,
	BlockSizeMismatch,
	Io(std::io::Error),
}

impl FileDev {
	/// Wrap a new file and emulate the given block size.
	pub fn new(mut file: File, block_size: BlockSize) -> Self {
		let len = file.seek(SeekFrom::End(0)).expect("failed to seek");
		Self { file: file.into(), block_size, block_count: len >> block_size.to_raw() }
	}

	fn seek(&self, lba: u64, len: usize) -> Result<(), FileDevError> {
		let end = lba.saturating_add((len >> self.block_size.to_raw()).try_into().unwrap());
		if len % (1 << self.block_size.to_raw()) != 0 {
			Err(FileDevError::BlockSizeMismatch)
		} else if end > self.block_count {
			Err(FileDevError::OutOfRange)
		} else {
			let offset = lba << self.block_size;
			self.file
				.borrow_mut()
				.seek(SeekFrom::Start(offset))
				.map(|_| ())
				.map_err(FileDevError::Io)
		}
	}
}

impl Dev for FileDev {
	type Error = FileDevError;
	type ReadTask<'a> = future::Ready<Result<FileBuf, Self::Error>>
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
		self.block_size
	}

	fn read<'a>(&self, lba: u64, len: usize) -> Self::ReadTask<'_> {
		future::ready(self.seek(lba, len).and_then(|()| {
			let mut buf = vec![0; len];
			self.file
				.borrow_mut()
				.read_exact(&mut buf)
				.map(|()| FileBuf(buf.into()))
				.map_err(FileDevError::Io)
		}))
	}

	fn write(&self, lba: u64, buf: <Self::Allocator as Allocator>::Buf) -> Self::WriteTask<'_> {
		future::ready(self.seek(lba, buf.0.len()).and_then(|()| {
			self.file
				.borrow_mut()
				.write_all(&buf.0)
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

impl fmt::Debug for FileDev {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(MemDev))
			.field("buf", &format_args!("[...]"))
			.field("block_size", &self.block_size)
			.finish()
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
	type Buf = FileBuf;

	fn alloc(&self, size: usize) -> Self::AllocTask<'_> {
		future::ready(Ok(FileBuf(vec![0; size].into())))
	}
}

#[derive(Clone)]
pub struct FileBuf(Arc<Vec<u8>>);

impl Buf for FileBuf {
	type Error = FileDevError;

	fn get(&self) -> &[u8] {
		&self.0
	}

	fn get_mut(&mut self) -> &mut [u8] {
		Arc::get_mut(&mut self.0).expect("buffer was cloned")
	}

	fn shrink(&mut self, len: usize) {
		assert!(len <= self.0.len(), "new len is larger than old len");
		Arc::get_mut(&mut self.0)
			.expect("buffer was cloned")
			.resize(len, 0);
	}
}
