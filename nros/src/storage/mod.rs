use alloc::boxed::Box;

pub trait Storage {
	type Error;

	fn block_size_p2(&self) -> u8;

	fn block_count(&self) -> u64;

	// TODO stop using Box as soon as GATs are somewhat stable.
	fn read(&mut self, lba: u64, blocks: usize) -> Result<Box<dyn Read + '_>, Self::Error>;

	fn write(
		&mut self,
		max_blocks: usize,
	) -> Result<Box<dyn Write<Error = Self::Error> + '_>, Self::Error>;

	fn fence(&mut self) -> Result<(), Self::Error>;
}

pub trait Read {
	fn get(&self) -> &[u8];
}

pub trait Write {
	type Error;

	fn get_mut(&mut self) -> &mut [u8];

	fn set_region(&mut self, lba: u64, blocks: usize) -> Result<(), Self::Error>;

	fn finish(self: Box<Self>) -> Result<(), Self::Error>;
}

/// A pseudo-device entirely in memory. Useful for testing.
#[derive(Debug)]
pub struct MemoryDev {
	buf: Box<[u8]>,
	block_size_p2: u8,
}

#[derive(Debug)]
pub enum MemoryDevError {
	OutOfRange,
}

impl MemoryDev {
	pub fn new(blocks: usize, block_size_p2: u8) -> Self {
		Self { buf: vec![0; blocks << block_size_p2].into(), block_size_p2 }
	}
}

impl Storage for MemoryDev {
	type Error = MemoryDevError;

	fn block_count(&self) -> u64 {
		self.buf.len() as u64 >> self.block_size_p2
	}

	fn block_size_p2(&self) -> u8 {
		self.block_size_p2
	}

	fn read(&mut self, lba: u64, blocks: usize) -> Result<Box<dyn Read + '_>, Self::Error> {
		struct R<'a>(&'a [u8]);
		impl Read for R<'_> {
			fn get(&self) -> &[u8] {
				self.0
			}
		}
		let start = (lba as usize) << self.block_size_p2;
		Ok(Box::new(R(
			&self.buf[start..][..blocks << self.block_size_p2]
		)))
	}

	fn write(
		&mut self,
		max_blocks: usize,
	) -> Result<Box<dyn Write<Error = Self::Error> + '_>, Self::Error> {
		Ok(Box::new(W {
			offset: usize::MAX,
			buf: vec![0; max_blocks << self.block_size_p2],
			dev: self,
		}))
	}

	fn fence(&mut self) -> Result<(), Self::Error> {
		Ok(())
	}
}

struct W<'a> {
	dev: &'a mut MemoryDev,
	offset: usize,
	buf: Vec<u8>,
}

impl Write for W<'_> {
	type Error = MemoryDevError;

	fn get_mut(&mut self) -> &mut [u8] {
		&mut self.buf
	}

	fn set_region(&mut self, lba: u64, blocks: usize) -> Result<(), Self::Error> {
		if lba + blocks as u64 > self.dev.buf.len() as u64 >> self.dev.block_size_p2 {
			return Err(MemoryDevError::OutOfRange);
		}
		self.offset = (lba as usize) << self.dev.block_size_p2;
		self.buf.resize(blocks << self.dev.block_size_p2, 0);
		Ok(())
	}

	fn finish(self: Box<Self>) -> Result<(), Self::Error> {
		self.dev.buf[self.offset..][..self.buf.len()].copy_from_slice(&self.buf);
		core::mem::forget(*self);
		Ok(())
	}
}
