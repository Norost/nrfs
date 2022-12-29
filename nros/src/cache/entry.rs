use {super::lru, core::fmt};

/// A single cache entry.
pub struct Entry {
	/// The data itself.
	pub data: Vec<u8>,
	/// Global LRU index.
	pub global_index: lru::Idx,
	/// Dirty LRU index, if the data is actually dirty.
	pub write_index: Option<lru::Idx>,
}

impl fmt::Debug for Entry {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Entry))
			.field("data", &format_args!("{:?}", &self.data))
			.field("global_index", &self.global_index)
			.field("write_index", &self.write_index)
			.finish()
	}
}
