use {
	super::{Child, DirSize, Hasher, MAX_LOAD_FACTOR_MILLI, MIN_LOAD_FACTOR_MILLI},
	crate::DataHeader,
	rangemap::RangeSet,
	rustc_hash::FxHashMap,
};

/// Directory data only, which has no lifetimes.
///
/// The map is located at ID.
/// The heap is located at ID + 1.
#[derive(Debug)]
pub(crate) struct DirData {
	/// Data header.
	pub(crate) header: DataHeader,
	/// Live [`FileRef`] and [`DirRef`]s that point to files which are a child of this directory.
	///
	/// Index corresponds to the position in the item list.
	pub(crate) children: FxHashMap<u32, Child>,
	/// Whether this directory has been removed and the corresponding item is dangling.
	///
	/// If `true`, no modifications may be made to this directory.
	pub(super) is_dangling: bool,
	/// The length of the header, in multiples of 8 bytes.
	pub(super) header_len8: u8,
	/// The length of a single item, in multiples of 8 bytes.
	pub(super) item_len8: u8,
	/// The size of the hashmap.
	pub(super) hashmap_size: DirSize,
	/// The hasher used to index the hashmap.
	pub(super) hasher: Hasher,
	/// The amount of entries in the hashmap.
	pub(super) item_count: u32,
	/// The amount of entries in the hashmap.
	pub(super) item_capacity: u32,
	/// The offset of `unix` extension data, if in use.
	pub(super) unix_offset: Option<u16>,
	/// The offset of `mtime` extension data, if in use.
	pub(super) mtime_offset: Option<u16>,
	/// Allocation map for the item list
	///
	/// It is lazily loaded to save time when only reading the directory.
	pub(super) item_alloc_map: Option<RangeSet<u32>>,
	/// Allocation map for the heap.
	///
	/// It is lazily loaded to save time when only reading the directory.
	pub(super) heap_alloc_map: Option<RangeSet<u64>>,
}

impl DirData {
	/// The size of a single item.
	pub(super) fn item_size(&self) -> u16 {
		u16::from(self.item_len8) * 8
	}

	/// The size of a single item.
	pub(super) fn header_len(&self) -> u16 {
		u16::from(self.header_len8) * 8
	}

	/// Check if the hashmap should grow.
	pub(super) fn should_grow(&self) -> bool {
		let cap = self.hashmap_capacity();
		let count = u64::from(self.item_count);
		cap - 1 == count || count * 1000 > cap * MAX_LOAD_FACTOR_MILLI
	}

	/// Check if the hashmap should shrink.
	pub(super) fn should_shrink(&self) -> bool {
		u64::from(self.item_count) * 1000 < self.hashmap_capacity() * MIN_LOAD_FACTOR_MILLI
	}

	/// The current size of the hashmap
	fn hashmap_capacity(&self) -> u64 {
		1u64 << self.hashmap_size
	}
}
