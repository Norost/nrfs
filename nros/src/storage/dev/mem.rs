use {
	super::{Allocator, Buf, Dev},
	crate::BlockSize,
	alloc::sync::Arc,
	core::{
		cell::{RefCell, RefMut},
		fmt, future,
	},
};

/// A pseudo-device entirely in memory. Useful for testing.
pub struct MemDev {
	buf: RefCell<Box<[u8]>>,
	block_size: BlockSize,
}

#[derive(Debug)]
pub enum MemDevError {
	OutOfRange,
	BlockSizeMismatch,
}

impl MemDev {
	pub fn new(blocks: usize, block_size: BlockSize) -> Self {
		Self {
			buf: vec![0; blocks << block_size.to_raw()]
				.into_boxed_slice()
				.into(),
			block_size,
		}
	}

	fn get_mut(&self, lba: u64, len: usize) -> Result<RefMut<'_, [u8]>, MemDevError> {
		let lba = usize::try_from(lba).map_err(|_| MemDevError::OutOfRange)?;
		let s = lba
			.checked_shl(self.block_size.to_raw().into())
			.ok_or(MemDevError::OutOfRange)?;
		let e = s.checked_add(len).ok_or(MemDevError::OutOfRange)?;
		RefMut::filter_map(self.buf.borrow_mut(), |b| b.get_mut(s..e))
			.map_err(|_| MemDevError::OutOfRange)
	}
}

impl Dev for MemDev {
	type Error = MemDevError;
	type ReadTask<'a> = future::Ready<Result<Arc<Vec<u8>>, Self::Error>>
	where
		Self: 'a;
	type WriteTask<'a> = future::Ready<Result<(), Self::Error>>
	where
		Self: 'a;
	type FenceTask<'a> = future::Ready<Result<(), Self::Error>>
	where
		Self: 'a;
	type Allocator = MemAllocator;

	fn block_count(&self) -> u64 {
		self.buf.borrow().len() as u64 >> self.block_size.to_raw()
	}

	fn block_size(&self) -> BlockSize {
		self.block_size
	}

	fn read<'a>(&self, lba: u64, len: usize) -> Self::ReadTask<'_> {
		future::ready(
			self.get_mut(lba, len)
				.map(|b| Arc::new(b.iter().copied().collect())),
		)
	}

	fn write(&self, lba: u64, buf: <Self::Allocator as Allocator>::Buf) -> Self::WriteTask<'_> {
		let res = self
			.get_mut(lba, buf.len())
			.map(|mut b| b.copy_from_slice(buf.get()));
		future::ready(res)
	}

	fn fence(&self) -> Self::FenceTask<'_> {
		future::ready(Ok(()))
	}

	fn allocator(&self) -> &Self::Allocator {
		&MemAllocator
	}
}

impl fmt::Debug for MemDev {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(MemDev))
			.field("buf", &format_args!("[...]"))
			.field("block_size", &self.block_size)
			.finish()
	}
}

/// Allocator returning heap buffers.
/// For use with [`MemDev`]
pub struct MemAllocator;

impl Allocator for MemAllocator {
	type Error = MemDevError;
	type AllocTask<'a> = future::Ready<Result<Self::Buf, Self::Error>>
	where
		Self: 'a;
	type Buf = Arc<Vec<u8>>;

	fn alloc(&self, size: usize) -> Self::AllocTask<'_> {
		future::ready(Ok(vec![0; size].into()))
	}
}
