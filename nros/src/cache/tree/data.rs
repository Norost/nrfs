use {
	super::super::{Key, Lru, MaxRecordSize, Present, RefCount, Slot, RECORD_SIZE_P2},
	crate::{resource::Buf, Record, Resource},
	core::fmt,
	rustc_hash::{FxHashMap, FxHashSet},
	std::collections::hash_map,
};

/// A single cached record tree.
pub(in super::super) struct TreeData<R: Resource> {
	/// The root of the tree.
	///
	/// The `_reserved` field is used to indicate whether the root is dirty or not.
	root: Record,
	/// Cached records.
	///
	/// The index in the array is correlated with depth.
	/// The key is correlated with offset.
	pub data: Box<[Level<R>]>,
}

impl<R: Resource> fmt::Debug for TreeData<R> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(TreeData))
			.field("root", &self.root)
			.field("data", &self.data)
			.finish()
	}
}

pub(in super::super) struct Level<R: Resource> {
	pub slots: FxHashMap<u64, Slot<R::Buf>>,
	pub dirty_markers: FxHashMap<u64, Dirty>,
}

#[derive(Debug, Default)]
pub(in super::super) struct Dirty {
	/// Whether the entry itself is dirty.
	pub is_dirty: bool,
	/// Children of the entry that have dirty descendants.
	pub children: FxHashSet<u64>,
}

impl<R: Resource> Default for Level<R> {
	fn default() -> Self {
		Self { slots: Default::default(), dirty_markers: Default::default() }
	}
}

impl<R: Resource> fmt::Debug for Level<R> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		struct FmtSlot<'a, R: Resource>(&'a Slot<R::Buf>);

		impl<R: Resource> fmt::Debug for FmtSlot<'_, R> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				match self.0 {
					Slot::Present(present) => format_args!("{:x?}", present.data.get()).fmt(f),
					Slot::Busy(busy) => busy.borrow_mut().fmt(f),
				}
			}
		}

		struct FmtSlots<'a, R: Resource>(&'a FxHashMap<u64, Slot<R::Buf>>);

		impl<R: Resource> fmt::Debug for FmtSlots<'_, R> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				let mut f = f.debug_map();
				for (k, v) in self.0.iter() {
					f.entry(k, &FmtSlot::<R>(v));
				}
				f.finish()
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

	/// Whether this object is dirty or not.
	pub fn is_dirty(&self) -> bool {
		self.root._reserved != 0
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
				let inserted = lvl
					.dirty_markers
					.entry(offt >> shift)
					.or_default()
					.children
					.insert(offt);
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
			debug_assert!(
				!marker.get().children.is_empty(),
				"non-dirty marker without children"
			);
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
			debug_assert!(_present, "no child");

			if !marker.get().is_dirty && marker.get().children.is_empty() {
				marker.remove();
			} else {
				break; // Nothing left to remove.
			}

			offt >>= shift;
		}

		true
	}

	/// Check if an entry is dirty.
	pub fn is_marked_dirty(&self, depth: u8, offset: u64) -> bool {
		let level = &self.data[usize::from(depth)];
		level.dirty_markers.get(&offset).is_some_and(|m| m.is_dirty)
	}

	/// Get the root of this object.
	pub fn root(&self) -> Record {
		Record { _reserved: 0, ..self.root }
	}

	/// Set the root of this object.
	pub fn set_root(&mut self, root: &Record) {
		self.root = Record { _reserved: 1, ..*root };
	}

	/// Clear the dirty status of this object.
	pub fn clear_dirty(&mut self) {
		self.root._reserved = 0;
	}

	#[cfg(debug_assertions)]
	#[track_caller]
	pub fn check_integrity(&self) {
		if let Some(Slot::Present(e)) = self.data.last().and_then(|l| l.slots.get(&0)) {
			let is_dirty = self
				.data
				.last()
				.and_then(|l| l.dirty_markers.get(&0))
				.is_some_and(|l| l.is_dirty);
			let must_be_empty = self.root.length == 0;
			if !is_dirty && (e.data.len() == 0) != must_be_empty {
				panic!("mismatch between root record and entry");
			}
		}
	}

	/// Transfer entries to a different ID.
	pub fn transfer_entries(&mut self, lru: &mut Lru, new_id: u64) {
		trace!("transfer_entries new_id {:#x}", new_id);
		// Fix entries
		for level in self.data.iter() {
			for slot in level.slots.values() {
				let f = |key: Key| {
					trace!(info "depth {} offset {}", key.depth(), key.offset());
					Key::new(0, new_id, key.depth(), key.offset())
				};
				match slot {
					Slot::Present(Present { refcount: RefCount::NoRef { lru_index }, .. }) => {
						let key = lru.get_mut(*lru_index).expect("not in lru");
						*key = f(*key)
					}
					Slot::Present(Present { refcount: RefCount::Ref { busy }, .. })
					| Slot::Busy(busy) => {
						let mut busy = busy.borrow_mut();
						busy.key = f(busy.key);
					}
				}
			}
		}
	}
}

impl<R: Resource> Present<TreeData<R>> {
	/// Add an entry.
	pub fn add_entry(&mut self, lru: &mut Lru, depth: u8, offset: u64, entry: Slot<R::Buf>) {
		let r = self.data.data[usize::from(depth)]
			.slots
			.insert(offset, entry);
		debug_assert!(r.is_none(), "entry already present");
		lru.object_increase_refcount(&mut self.refcount);
	}
}
