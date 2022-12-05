use {
	super::{Allocator, Buf, Dev},
	crate::BlockSize,
	core::{
		cell::{RefCell, RefMut},
		future,
		ops::Range,
	},
	std::rc::Rc,
};

/// A pseudo-device entirely in memory. Useful for testing.
#[derive(Debug)]
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

	fn get_mut(&self, lba: u64, len: usize) -> Result<RefMut<[u8]>, MemDevError> {
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
	type ReadTask<'a> = future::Ready<Result<MemBuf, Self::Error>>
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
				.map(|b| MemBuf(b.iter().copied().collect())),
		)
	}

	fn write(
		&self,
		lba: u64,
		buf: <Self::Allocator as Allocator>::Buf<'_>,
		range: Range<usize>,
	) -> Self::WriteTask<'_> {
		let res = self
			.get_mut(lba, buf.0.len())
			.map(|mut b| b.copy_from_slice(&buf.get()[range]));
		future::ready(res)
	}

	fn fence(&self) -> Self::FenceTask<'_> {
		future::ready(Ok(()))
	}

	fn allocator(&self) -> &Self::Allocator {
		&MemAllocator
	}
}

/// Allocator returning heap buffers.
/// For use with [`MemDev`]
pub struct MemAllocator;

impl Allocator for MemAllocator {
	type Error = MemDevError;
	type AllocTask<'a> = future::Ready<Result<Self::Buf<'a>, Self::Error>>
	where
		Self: 'a;
	type Buf<'a> = MemBuf
	where
		Self: 'a;

	fn alloc(&self, size: usize) -> Self::AllocTask<'_> {
		future::ready(Ok(MemBuf(vec![0; size].into())))
	}
}

#[derive(Clone)]
pub struct MemBuf(Rc<[u8]>);

impl Buf for MemBuf {
	type Error = MemDevError;

	fn get(&self) -> &[u8] {
		&self.0
	}

	fn get_mut(&mut self) -> &mut [u8] {
		Rc::get_mut(&mut self.0).expect("buffer was cloned")
	}
}

pub struct MemRead {
	buf: Box<[u8]>,
}
