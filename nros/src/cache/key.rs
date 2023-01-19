use {
	super::{MaxRecordSize, Resource, TreeData, OBJECT_LIST_ID},
	crate::cache::RECORD_SIZE_P2,
	core::fmt,
	rustc_hash::FxHashMap,
	std::collections::hash_map,
};

/// Key for indexing in the cache.
///
/// This is more optimized than using a `(u64, u8, u64)` tuple.
/// It exploits the following observations:
///
/// * offset is between `0` and `2**64 / (2**9 / 2**5) - 1 = 2**59 - 1`
///   `2**64` is the range of a `u64` offset.
///   `2**9` is the smallest maximum size of a single record.
///   `2**5` is the size of a `Record`.
/// * depth is no more than 14 (assume block size = 512 -> `9 + 4 * 14 = 65`, just enough).
/// * There can be at most 2**59 objects (due to 2**5 record size).
///
/// Ergo, we need 59 + 4 + 59 = 122 bits at most.
/// The 4 depth bits are put in the high bits of the offset.
/// The 5 free bits in the ID are used for "pseudo-IDs", i.e. IDs for objects that aren't stored
/// on disk.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(u64, u64);

impl Key {
	/// # Panics
	///
	/// If `offset` (< 2**59) or `depth` (< 14) are out of range.
	pub fn new(id: u64, depth: u8, offset: u64) -> Self {
		assert!(offset < 1 << 59, "offset out of range");
		assert!(depth <= 14, "depth out of range");
		Self(id, offset << 4 | u64::from(depth))
	}

	pub fn id(&self) -> u64 {
		self.0
	}

	pub fn depth(&self) -> u8 {
		(self.1 & 0xf) as _
	}

	pub fn offset(&self) -> u64 {
		self.1 >> 4
	}
}

impl fmt::Debug for Key {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Key))
			.field("id", &self.id())
			.field("depth", &self.depth())
			.field("offset", &self.offset())
			.finish()
	}
}
