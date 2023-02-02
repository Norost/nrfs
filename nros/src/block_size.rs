n2e! {
	[BlockSize]
	9 B512
	10 K1
	11 K2
	12 K4
	13 K8
	14 K16
	15 K32
	16 K64
	17 K128
	18 K256
	19 K512
	20 M1
	21 M2
	22 M4
	23 M8
	24 M16
}

impl BlockSize {
	/// Calculate the minimum amount of blocks necessary to contain the given amount of bytes.
	pub(crate) fn min_blocks(&self, n: usize) -> usize {
		let mask = (1 << self.to_raw()) - 1;
		(n + mask) >> self.to_raw()
	}

	/// Round up the given amount of bytes to a multiple of the block size.
	pub(crate) fn round_up(&self, n: usize) -> usize {
		let mask = (1 << self.to_raw()) - 1;
		(n + mask) & !mask
	}
}

impl Default for BlockSize {
	fn default() -> Self {
		Self::K4
	}
}
