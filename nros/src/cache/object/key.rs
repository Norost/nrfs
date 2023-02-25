use {
	crate::data::record::Depth,
	core::{fmt, iter::Step},
};

/// Key for indexing in the cache.
///
/// This is more efficient than using a `(u8, u8, u64)` tuple.
/// It exploits the following observations:
///
/// * offset is between `0` and `2**64 / 2**9 - 1 = 2**55 - 1`
///   `2**9` is the smallest maximum size of a single record.
///   `2**3` is the size of a `Record`.
/// * depth is no more than 4.
/// * there are only 4 roots.
///
/// Ergo, we need 2 + 2 + 55 = 59 bits at most.
/// The 2 depth bits are put in the high bits of the offset.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key(u64);

impl Key {
	/// The maximum valid offset.
	pub const MAX_OFFSET: u64 = (1 << 55) - 1;

	/// # Panics
	///
	/// If offset is out of range, i.e. equal to or larger than `2**55`.
	pub fn new(root: RootIndex, depth: Depth, offset: u64) -> Self {
		assert!(offset < 1 << 55, "offset out of range");
		Self((root as u64) << 62 | (depth as u64) << 60 | offset)
	}

	pub fn root(&self) -> RootIndex {
		match (self.0 >> 62) & 3 {
			0 => RootIndex::I0,
			1 => RootIndex::I1,
			2 => RootIndex::I2,
			3 => RootIndex::I3,
			_ => unreachable!(),
		}
	}

	pub fn depth(&self) -> Depth {
		match (self.0 >> 60) & 3 {
			0 => Depth::D0,
			1 => Depth::D1,
			2 => Depth::D2,
			3 => Depth::D3,
			_ => unreachable!(),
		}
	}

	pub fn offset(&self) -> u64 {
		self.0 & !(0xf << 60)
	}
}

impl fmt::Debug for Key {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let k = self;
		format_args!("({:?}:{:?}:{})", k.root(), k.depth(), k.offset()).fmt(f)
	}
}

/// The index of the root of the tree the record belongs to;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RootIndex {
	I0 = 0,
	I1 = 1,
	I2 = 2,
	I3 = 3,
}

impl RootIndex {
	fn i2d(i: usize) -> Option<Self> {
		[Self::I0, Self::I1, Self::I2, Self::I3].get(i).copied()
	}

	/// The depth of the corresponding tree.
	pub fn depth(self) -> Depth {
		Depth::try_from(self as u8).unwrap()
	}
}

impl Step for RootIndex {
	fn steps_between(start: &Self, end: &Self) -> Option<usize> {
		(*end as usize).checked_sub(*start as usize)
	}

	fn forward_checked(start: Self, count: usize) -> Option<Self> {
		(start as usize).checked_add(count).and_then(Self::i2d)
	}

	fn backward_checked(start: Self, count: usize) -> Option<Self> {
		(start as usize).checked_sub(count).and_then(Self::i2d)
	}
}
