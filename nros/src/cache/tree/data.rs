use {
	super::super::{
		memory_tracker::MemoryTracker, Key, MaxRecordSize, Object, Present, RefCount, Slot,
		RECORD_SIZE_P2,
	},
	crate::resource::Buf,
	alloc::collections::{
		btree_map::{self, OccupiedEntry},
		BTreeMap, BTreeSet,
	},
	core::fmt,
};

/// A single cached record tree.
pub(in super::super) struct TreeData<B: Buf> {
	/// The object field of this tree.
	///
	/// `_reserved[0]` is used to indicate whether the object is dirty or not.
	object: Object,
	/// Cached records.
	///
	/// The index in the array is correlated with depth.
	/// The key is correlated with offset.
	data: Box<[Level<B>]>,
}

pub(in super::super) struct Level<B> {
	pub slots: BTreeMap<u64, Slot<B>>,
	pub dirty_markers: BTreeMap<u64, Dirty>,
}

#[derive(Debug, Default)]
pub(in super::super) struct Dirty {
	/// Whether the entry itself is dirty.
	pub is_dirty: bool,
	/// Children of the entry that have dirty descendants.
	pub children: BTreeSet<u64>,
}

impl<B> Default for Level<B> {
	fn default() -> Self {
		Self { slots: Default::default(), dirty_markers: Default::default() }
	}
}

impl<B: Buf> fmt::Debug for TreeData<B> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(TreeData))
			.field("object", &self.object)
			.field("data", &self.data)
			.finish()
	}
}

impl<B: Buf> fmt::Debug for Level<B> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		struct FmtPresent<'a, B>(&'a Present<B>);

		impl<B: Buf> fmt::Debug for FmtPresent<'_, B> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				f.debug_struct(stringify!(Present))
					.field("data", &format_args!("{:x?}", self.0.data.get()))
					.field("refcount", &self.0.refcount)
					.finish()
			}
		}

		struct FmtSlot<'a, B>(&'a Slot<B>);

		impl<B: Buf> fmt::Debug for FmtSlot<'_, B> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				match self.0 {
					Slot::Present(present) => FmtPresent(present).fmt(f),
					Slot::Busy(busy) => busy.borrow_mut().fmt(f),
				}
			}
		}

		struct FmtSlots<'a, B>(&'a BTreeMap<u64, Slot<B>>);

		impl<B: Buf> fmt::Debug for FmtSlots<'_, B> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				let mut f = f.debug_map();
				for (k, v) in self.0.iter() {
					f.entry(k, &FmtSlot::<B>(v));
				}
				f.finish()
			}
		}

		f.debug_struct(stringify!(Level))
			.field("slots", &FmtSlots::<B>(&self.slots))
			.field("dirty_markers", &self.dirty_markers)
			.finish()
	}
}

impl<B: Buf> TreeData<B> {
	pub fn new(object: Object, max_record_size: MaxRecordSize) -> Self {
		let depth = super::depth(max_record_size, object.total_length());
		Self { object, data: (0..depth).map(|_| Default::default()).collect() }
	}

	/// Whether this object is dirty or not.
	pub fn is_dirty(&self) -> bool {
		self.object._reserved != 0
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

		let btree_map::Entry::Occupied(mut marker) = level.dirty_markers.entry(offset)
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
			let btree_map::Entry::Occupied(mut marker) = lvl.dirty_markers.entry(offt >> shift)
				else { unreachable!("no marker") };

			let present = marker.get_mut().children.remove(&offt);
			debug_assert!(present, "no child");

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

	/// Get the object field.
	pub fn object(&self) -> Object {
		Object { _reserved: 0, ..self.object }
	}

	/// Set the object of this object.
	pub fn set_object(&mut self, object: &Object) {
		self.object = Object { _reserved: 1, ..*object };
	}

	/// Clear the dirty status of this object.
	pub fn clear_dirty(&mut self) {
		self.object._reserved = 0;
	}

	/// Transfer entries to a different ID.
	pub fn transfer_entries(&mut self, memory_tracker: &mut MemoryTracker, new_id: u64) {
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
						let key = memory_tracker.get_key_mut(*lru_index).expect("not in lru");
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

	pub fn data_mut(&mut self) -> &mut Box<[Level<B>]> {
		&mut self.data
	}
}

impl<B: Buf> Present<TreeData<B>> {
	#[cfg(debug_assertions)]
	#[track_caller]
	pub fn check_integrity(&self) {
		// Check total entry count.
		// Every entry must reference its corresponding object.
		let refcount = match &self.refcount {
			RefCount::NoRef { .. } => 0,
			RefCount::Ref { busy } => busy.borrow_mut().refcount,
		};
		let entry_count = self.data.data.iter().fold(0, |x, l| x + l.slots.len());
		assert!(refcount >= entry_count);

		// Check top *non*-dirty entry & root
		if let Some(Slot::Present(e)) = self.data.data.last().and_then(|l| l.slots.get(&0)) {
			let is_dirty = self
				.data
				.data
				.last()
				.and_then(|l| l.dirty_markers.get(&0))
				.is_some_and(|l| l.is_dirty);
			let must_be_empty = self.data.object.root.length() == 0;
			if !is_dirty && (e.data.len() == 0) != must_be_empty {
				panic!("mismatch between object record and entry");
			}
		}
	}

	/// Insert a new entry.
	pub fn insert_entry(
		&mut self,
		memory_tracker: &mut MemoryTracker,
		depth: u8,
		offset: u64,
		entry: Slot<B>,
	) {
		trace!("TreeData::insert_entry {}:{}", depth, offset);
		let prev = self.data.data[usize::from(depth)]
			.slots
			.insert(offset, entry);
		memory_tracker.incr_object_refcount(&mut self.refcount, 1);
		debug_assert!(prev.is_none(), "slot was occupied");
	}

	/// Get an entry.
	pub fn get_mut(&mut self, depth: u8, offset: u64) -> Option<&mut Slot<B>> {
		self.data.data[usize::from(depth)].slots.get_mut(&offset)
	}

	/// Get an entry.
	pub fn occupied(&mut self, depth: u8, offset: u64) -> Option<OccupiedEntry<'_, u64, Slot<B>>> {
		let entry = self.data.data[usize::from(depth)].slots.entry(offset);
		let btree_map::Entry::Occupied(entry) = entry else { return None };
		Some(entry)
	}

	/// Remove an entry.
	pub fn remove_entry(&mut self, memory_tracker: &mut MemoryTracker, depth: u8, offset: u64) {
		let prev = self.data.data[usize::from(depth)].slots.remove(&offset);
		debug_assert!(prev.is_some(), "slot was empty");
		memory_tracker.decr_object_refcount(&mut self.refcount, 1);
	}

	/// Get a level.
	pub fn level_mut(&mut self, depth: u8) -> &mut Level<B> {
		&mut self.data.data[usize::from(depth)]
	}

	/// Get a level.
	pub fn try_level_mut(&mut self, depth: u8) -> Option<&mut Level<B>> {
		self.data.data.get_mut(usize::from(depth))
	}

	/// Get levels at & above `depth`.
	pub fn levels_mut(&mut self, depth: u8) -> (&mut Level<B>, &mut [Level<B>]) {
		let Some([level, levels @ ..]) = self.data.data.get_mut(usize::from(depth)..)
			else { panic!("out of range") };
		(level, levels)
	}

	pub fn data(&self) -> &[Level<B>] {
		&self.data.data
	}

	pub fn data_mut(&mut self) -> &mut Box<[Level<B>]> {
		&mut self.data.data
	}

	pub fn set_dirty(&mut self, value: bool) {
		self.data.object._reserved = value.into()
	}
}
