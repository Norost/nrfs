use super::{Buf, Dev, Allocator};
use std::rc::Rc;
use core::cell::{RefCell, RefMut};
use core::future;

/// A pseudo-device entirely in memory. Useful for testing.
#[derive(Debug)]
pub struct MemDev {
	buf: RefCell<Box<[u8]>>,
	block_size_p2: u8,
	/// An unhandled error that may have occured during [`Self::write`].
	write_error: Option<MemDevError>,
}

#[derive(Debug)]
pub enum MemDevError {
	OutOfRange,
	BlockSizeMismatch,
}

impl MemDev {
	pub fn new(blocks: usize, block_size_p2: u8) -> Self {
		Self {
			buf: vec![0; blocks << block_size_p2].into_boxed_slice().into(),
			block_size_p2,
			write_error: None,
		}
	}

	fn get_mut(&self, lba: u64, len: usize) -> Result<RefMut<[u8]>, MemDevError> {
		let lba = usize::try_from(lba).map_err(|_| MemDevError::OutOfRange)?;
		let s = lba
			.checked_shl(self.block_size_p2.into())
			.ok_or(MemDevError::OutOfRange)?;
		let e = s.checked_add(len).ok_or(MemDevError::OutOfRange)?;
		RefMut::filter_map(self.buf.borrow_mut(), |b| b.get_mut(s..e)).map_err(|_| MemDevError::OutOfRange)
	}
}

impl Dev for MemDev {
	type Error = MemDevError;
	type ReadTask = future::Ready<Result<MemBuf, Self::Error>>;
	type FenceTask = future::Ready<Result<(), Self::Error>>;
	type Allocator = MemAllocator;

	fn block_count(&self) -> u64 {
		self.buf.borrow().len() as u64 >> self.block_size_p2
	}

	fn read<'a>(
		&self,
		lba: u64,
		len: usize,
	) -> Self::ReadTask<'_> {
		future::ready(self.get_mut(lba, len).map(|b| MemBuf(b.iter().copied().collect())))
	}

	fn write(&self, lba: u64, buf: <Self::Allocator as Allocator>::Buf<'_>) {
		if self.write_error.is_none() {
			self.get_mut(lba, buf.0.len())
				.map(|b| b.copy_from_slice(buf.get()))
				.map_err(|e| self.write_error = Some(e));
		}
	}

	fn fence(&self) -> Self::FenceTask<'_> {
		future::ready(self.write_error.take().map_or(Ok(()), Err))
	}
}

/// Allocator returning heap buffers.
/// For use with [`MemDev`]
pub struct MemAllocator;

impl Allocator for MemAllocator {
	type Error = MemDevError;
	type Buf<'a> = MemBuf
	where
		Self: 'a;
}

pub struct MemBuf(Rc<[u8]>);

impl Buf for MemBuf {
	type Error = MemDevError;
	type MutTask<'a> = future::Ready<Result<&'a mut [u8], Self::Error>>
	where
		Self: 'a;

	fn get(&self) -> &[u8] {
		&self.0
	}

	fn get_mut(&mut self) -> Self::MutTask<'_> {
		if let Some(buf) = Rc::get_mut(&mut self.0) {
			return future::ready(Ok(buf));
		}
		// Deep clone
		self.0 = self.0.iter().copied().collect();
		future::ready(Ok(Rc::get_mut(&mut self.0).unwrap()))
	}
}

pub struct MemRead {
	buf: Box<[u8]>,
}
