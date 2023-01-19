use {
	super::super::{MaxRecordSize, Slot, RECORD_SIZE_P2},
	crate::{Record, Resource},
	std::collections::hash_map,
	core::fmt,
	rustc_hash::FxHashMap,
	rustc_hash::FxHashSet,
};

const MAX_DEPTH: u8 = 14;

/// A single cached record tree.
#[derive(Debug)]
pub struct TreeData<R: Resource> {
	/// The root of the tree.
	///
	/// The `_reserved` field is used to indicate whether the root is dirty or not.
	pub(in super::super) root: Record,
	/// Cached records.
	///
	/// The index in the array is correlated with depth.
	/// The key is correlated with offset.
	pub(in super::super) data: Box<[Level<R>]>,
}

pub struct Level<R: Resource> {
	pub(in super::super) slots: FxHashMap<u64, Slot<R::Buf>>,
	pub(in super::super) dirty_markers: FxHashMap<u64, Dirty>,
}

#[derive(Debug, Default)]
pub struct Dirty {
	/// Whether the entry itself is dirty.
	pub(super) is_dirty: bool,
	/// Children of the entry that have dirty descendants.
	pub(super) children: FxHashSet<u64>,
}

impl<R: Resource> Default for Level<R> {
	fn default() -> Self {
		Self { slots: Default::default(), dirty_markers: Default::default() }
	}
}

impl<R: Resource> fmt::Debug for Level<R> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		struct FmtSlots<'a, R: Resource>(&'a FxHashMap<u64, Slot<R::Buf>>);

		impl<R: Resource> fmt::Debug for FmtSlots<'_, R> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				f.debug_struct(stringify!(Slot)).finish_non_exhaustive()
			}
		}

		f.debug_struct(stringify!(Level))
			.field("slots", &FmtSlots::<R>(&self.slots))
			.field("dirty_markers", &self.dirty_markers)
			.finish()
	}
}

impl<R: Resource> TreeData<R> {
	pub fn new(root: Record, max_record_size: MaxRecordSize) -> Self {
		let depth = super::depth(max_record_size, root.total_length.into());
		Self { root, data: (0..depth).map(|_| Default::default()).collect() }
	}

	/// Mark an entry as dirty.
	pub fn mark_dirty(&mut self, depth: u8, offset: u64, max_record_size: MaxRecordSize) {
		let [level, levels @ ..] = &mut self.data[usize::from(depth)..]
			else { panic!("depth out of range") };

		let marker = level.dirty_markers.entry(offset).or_default();
		if !marker.is_dirty {
			marker.is_dirty = true;

			let shift = max_record_size.to_raw() - RECORD_SIZE_P2;
			let mut offt = offset;

			for lvl in levels {
				let inserted = lvl.dirty_markers.entry(offt >> shift).or_default().children.insert(offt);
				if !inserted {
					// The entry was already present.
					break;
				}
				offt >>= shift;
			}
		}
	}

	/// Unmark an entry as dirty.
	///
	/// Returns `true` if the entry was dirty.
	pub fn unmark_dirty(&mut self, depth: u8, offset: u64, max_record_size: MaxRecordSize) -> bool {
		let [level, levels @ ..] = &mut self.data[usize::from(depth)..]
			else { panic!("depth out of range") };

		let hash_map::Entry::Occupied(mut marker) = level.dirty_markers.entry(offset)
			else { return false };

		if !marker.get().is_dirty {
			debug_assert!(!marker.get().children.is_empty(), "non-dirty marker without children");
			return false;
		}

		marker.get_mut().is_dirty = false;
		if marker.get().children.is_empty() {
			marker.remove();
		} else {
			return true; // Nothing left to remove.
		}

		let shift = max_record_size.to_raw() - RECORD_SIZE_P2;
		let mut offt = offset;

		for lvl in levels {
			let hash_map::Entry::Occupied(mut marker) = lvl.dirty_markers.entry(offt >> shift)
				else { unreachable!("no marker") };

			let _present = marker.get_mut().children.remove(&offt);
			debug_assert!(_present);

			if !marker.get().is_dirty && marker.get().children.is_empty() {
				marker.remove();
			} else {
				break; // Nothing left to remove.
			}

			offt >>= shift;
		}

		true
	}
}
