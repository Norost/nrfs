use {
	super::{Child, Index, Offset, MAX_LOAD_FACTOR_MILLI, MIN_LOAD_FACTOR_MILLI},
	crate::DataHeader,
	alloc::collections::BTreeMap,
	rangemap::RangeSet,
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
	pub(crate) children: BTreeMap<Index, Child>,
	/// Whether this directory has been removed and the corresponding item is dangling.
	///
	/// If `true`, no modifications may be made to this directory.
	pub(super) is_dangling: bool,
	/// The length of the header, in multiples of 8 bytes.
	pub(super) header_len8: u8,
	/// The length of a single item, in multiples of 8 bytes.
	pub(super) item_len8: u8,
	/// The amount of entries in the hashmap.
	pub(super) item_count: u32,
	/// Allocation map for the heap.
	///
	/// It is lazily loaded to save time when only reading the directory.
	pub(super) heap_alloc_map: Option<RangeSet<Offset>>,
	/// Enabled extensions.
	ext_enabled: u8,
	/// The offset of `mtime` extension data in the item metadata, if enabled.
	ext_mtime_offset: u16,
	/// The offset of `unix` extension data in the item metadata, if enabled.
	ext_unix_offset: u16,
}

impl DirData {
	const EXT_MTIME_FLAG: u8 = 1 << 0;
	const EXT_UNIX_FLAG: u8 = 1 << 1;

	/// The size of the directory header in bytes.
	pub(super) fn header_len(&self) -> u16 {
		u16::from(self.header_len8) * 8
	}

	/// The size of a single item in bytes.
	pub(super) fn item_size(&self) -> u16 {
		u16::from(self.item_len8) * 8
	}

	/// The offset of `mtime` extension data in the item metadata, if enabled.
	pub(super) fn mtime_offset(&self) -> Option<u16> {
		(self.ext_enabled & Self::EXT_MTIME_FLAG != 0).then(|| self.ext_mtime_offset)
	}

	/// The offset of `unix` extension data in the item metadata, if enabled.
	pub(super) fn unix_offset(&self) -> Option<u16> {
		(self.ext_enabled & Self::EXT_UNIX_FLAG != 0).then(|| self.ext_unix_offset)
	}
}
