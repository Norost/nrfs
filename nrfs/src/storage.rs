use alloc::boxed::Box;

pub trait Storage {
	type Error;

	fn block_size_p2(&self) -> u8;

	fn block_count(&self) -> u64;

	// TODO stop using Box as soon as GATs are somewhat stable.
	fn read(&mut self, start: u64, blocks: usize) -> Result<Box<dyn Read + '_>, Self::Error>;

	fn write(&mut self, start: u64, blocks: usize) -> Result<Box<dyn Write + '_>, Self::Error>;

	fn fence(&mut self) -> Result<(), Self::Error>;
}

pub trait Read {
	fn get(&self) -> &[u8];
}

pub trait Write {
	fn get_mut(&mut self) -> &mut [u8];

	fn set_blocks(&mut self, n: usize);
}
