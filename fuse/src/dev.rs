use {
	nrfs::dev::Allocator,
	std::{fs::File, io::Seek, os::unix::fs::FileExt, sync::Arc},
};

#[derive(Debug)]
pub struct Dev {
	file: Arc<File>,
	block_count: u64,
}

impl Dev {
	pub fn new(mut file: File) -> Self {
		let block_count = file.stream_position().unwrap();
		Self { file: file.into(), block_count }
	}
}

impl nrfs::Dev for Dev {
	type Error = std::io::Error;
	type Allocator = Alloc;
	type ReadTask<'a> = blocking::Task<Result<Arc<Vec<u8>>, Self::Error>>
	where
		Self: 'a;
	type WriteTask<'a> = blocking::Task<Result<(), Self::Error>>
	where
		Self: 'a;
	type FenceTask<'a> = blocking::Task<Result<(), Self::Error>>
	where
		Self: 'a;

	fn read(&self, offset: u64, len: usize) -> Self::ReadTask<'_> {
		let file = self.file.clone();
		blocking::unblock(move || {
			let mut buf = vec![0; len];
			file.read_exact_at(&mut buf, offset << 9)?;
			Ok(buf.into())
		})
	}

	fn write(&self, offset: u64, data: Arc<Vec<u8>>) -> Self::WriteTask<'_> {
		let file = self.file.clone();
		blocking::unblock(move || file.write_all_at(&data, offset << 9))
	}

	fn fence(&self) -> Self::FenceTask<'_> {
		let file = self.file.clone();
		blocking::unblock(move || file.sync_data())
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
