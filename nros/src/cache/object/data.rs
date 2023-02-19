use {
	super::super::{Entry, Key},
	crate::resource::Buf,
	alloc::collections::{BTreeMap, BTreeSet},
	core::fmt,
};

/// A single cached record object.
pub(in super::super) struct ObjectData<B: Buf> {
	/// Cached records.
	///
	/// The index in the array is correlated with depth.
	/// The key is correlated with offset.
	pub records: BTreeMap<Key, Entry<B>>,
	/// Dirty record markers.
	///
	/// Used to track records which are dirty or have dirty descendants.
	pub dirty_records: BTreeSet<Key>,
}

impl<B: Buf> Default for ObjectData<B> {
	fn default() -> Self {
		Self { records: Default::default(), dirty_records: Default::default() }
	}
}

impl<B: Buf> fmt::Debug for ObjectData<B> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(ObjectData))
			.field("records", &self.records)
			.field("dirty_records", &self.dirty_records)
			.finish()
	}
}
