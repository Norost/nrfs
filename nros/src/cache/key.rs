use {
	super::{Entry, MaxRecordSize, TreeData, OBJECT_LIST_ID},
	crate::cache::RECORD_SIZE_P2,
	core::fmt,
	rustc_hash::FxHashMap,
};

/// Key for indexing in the cache.
///
/// This is more optimized than using a `(u64, u8, u64)` tuple.
/// It exploits the following observations:
///
/// * offset is between 0 and 2**64 - 1
/// * depth is no more than 14 (assume block size = 512 -> `9 + 4 * 14 = 65`, just enough).
/// * There can be at most 2**59 objects (due to 2**5 record size).
///
/// Ergo, we need 64 + 4 + 59 = 127 bits at most.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(u64, u64);

impl Key {
	/// # Panics
	///
	/// If `id` (< 2**59) or `depth` (< 14) are out of range.
	pub fn new(id: u64, depth: u8, offset: u64) -> Self {
		assert!(id < 1 << 59 || id == OBJECT_LIST_ID, "id out of range");
		assert!(depth < 14, "depth out of range");
		Self(id << 4 | u64::from(depth), offset)
	}

	pub fn id(&self) -> u64 {
		self.0 >> 4
	}

	pub fn depth(&self) -> u8 {
		(self.0 & 0xf) as u8
	}

	pub fn offset(&self) -> u64 {
		self.1
	}

	/// Use this `Key` to get a mutable reference to an entry in a cache.
	///
	/// # Panics
	///
	/// If `depth` is out of range.
	pub fn get_entry_mut<'a>(
		&self,
		data: &'a mut FxHashMap<u64, TreeData>,
	) -> Option<&'a mut Entry> {
		data.get_mut(&self.id())
			.map(|tree| &mut tree.data[usize::from(self.depth())])
			.and_then(|level| level.entries.get_mut(&self.offset()))
	}

	/// Use this `Key` to insert an entry in a cache.
	///
	/// # Panics
	///
	/// If an entry was already present.
	pub fn insert_entry<'a>(
		&self,
		data: &'a mut FxHashMap<u64, TreeData>,
		max_depth: u8,
		entry: Entry,
	) -> &'a mut Entry {
		data.entry(self.id())
			.or_insert(TreeData::new(max_depth))
			.data[usize::from(self.depth())]
		.entries
		.try_insert(self.offset(), entry)
		.expect("entry was already present")
	}

	/// Use this `Key` to remove an entry in a cache.
	///
	/// # Panics
	///
	/// If `depth` is out of range.
	pub fn remove_entry<'a>(
		&self,
		max_record_size: MaxRecordSize,
		data: &'a mut FxHashMap<u64, TreeData>,
	) -> Option<Entry> {
		let tree = data.get_mut(&self.id())?;
		let level = &mut tree.data[usize::from(self.depth())];
		let entry = level.entries.remove(&self.offset())?;
		// If the tree has no cached records left, remove it.
		if level.entries.is_empty() && tree.is_empty() {
			data.remove(&self.id());
		} else if entry.write_index.is_some() {
			// Update dirty counters if the entry was dirty.
			let mut offt = self.offset();
			for level in
				data.get_mut(&self.id()).expect("no tree").data[self.depth().into()..].iter_mut()
			{
				let std::collections::hash_map::Entry::Occupied(mut c) = level.dirty_counters.entry(offt)
					else { panic!("no dirty counter") };
				*c.get_mut() -= 1;
				if *c.get() == 0 {
					c.remove();
				}
				offt >>= max_record_size.to_raw() - RECORD_SIZE_P2;
			}
		}
		Some(entry)
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
