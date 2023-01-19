pub mod data;
mod fetch;

use {
	super::{
		slot::RefCount, Busy, Cache, EntryRef, Key, Present, Slot, CACHE_ENTRY_FIXED_COST,
		OBJECT_LIST_ID, RECORD_SIZE_P2,
	},
	crate::{
		resource::Buf, util::get_record, Background, Dev, Error, MaxRecordSize, Record, Resource,
	},
	core::{cell::RefMut, future::Future, mem, num::NonZeroUsize, ops::RangeInclusive, pin::Pin},
};

/// Implementation of a record tree.
///
/// As long as a `Tree` object for a specific ID is alive its [`TreeData`] entry will not be
/// evicted.
#[derive(Clone, Debug)]
pub struct Tree<'a, 'b, D: Dev, R: Resource> {
	/// Underlying cache.
	cache: &'a Cache<D, R>,
	/// Background task runner.
	background: &'b Background<'a, D>,
	/// ID of the object.
	id: u64,
}

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
	/// Access a tree.
	pub(super) fn new(
		cache: &'a Cache<D, R>,
		bg: &'b Background<'a, D>,
		id: u64,
	) -> Tree<'a, 'b, D, R> {
		Self { cache, background: bg, id }
	}

	/// Write data to a range.
	///
	/// Returns the actual amount of bytes written.
	/// It may exit early if the necessary data is not cached (e.g. partial record write)
	pub async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, Error<D>> {
		trace!(
			"write id {}, offset {}, len {}",
			self.id,
			offset,
			data.len()
		);

		let (root, len) = self.root().await?;
		let max_depth = depth(self.max_record_size(), len);

		// Ensure all data fits.
		let data = if offset >= len {
			return Ok(0);
		} else if offset.saturating_add(u64::try_from(data.len()).unwrap()) >= len {
			&data[..usize::try_from(len - offset).unwrap()]
		} else {
			data
		};

		if data.is_empty() {
			return Ok(0);
		}

		let range = calc_range(self.max_record_size(), offset, data.len());
		let (first_offset, last_offset) =
			calc_record_offsets(self.max_record_size(), offset, data.len());

		if range.start() == range.end() {
			// We need to slice one record twice
			let entry = self.get(0, *range.start()).await?;
			entry.modify(self.background, |b| {
				let min_len = last_offset.max(b.len());
				b.resize(min_len, 0);
				b.get_mut()[first_offset..last_offset].copy_from_slice(data);
			});
		} else {
			// We need to slice the first & last record once and operate on the others in full.
			let mut data = data;
			let mut range = range.into_iter();

			let first_key = range.next().unwrap();
			let last_key = range.next_back().unwrap();

			// Copy to first record |----xxxx|
			{
				let d;
				(d, data) = data.split_at((1usize << self.max_record_size()) - first_offset);

				let entry = self.get(0, first_key).await?;
				entry.modify(self.background, |b| {
					b.resize(first_offset, 0);
					b.extend_from_slice(d);
				});
			}

			// Copy middle records |xxxxxxxx|
			for offset in range {
				let d;
				(d, data) = data.split_at(1usize << self.max_record_size());

				// "Fetch" directly since we're overwriting the entire record anyways.
				let key = Key::new(0, self.id, 0, offset);
				let entry = self
					.cache
					.fetch_entry(self.background, key, &Record::default())
					.await?;

				entry.modify(self.background, |b| {
					// If the record was already fetched, it'll have ignored the &Record::default().
					// Hence we need to clear it manually.
					b.resize(0, 0);
					b.extend_from_slice(d);
				});
			}

			// Copy end record |xxxx----|
			// Don't bother if there is no data
			if last_offset > 0 {
				debug_assert_eq!(data.len(), last_offset);
				let entry = self.get(0, last_key).await?;
				entry.modify(self.background, |b| {
					let min_len = b.len().max(data.len());
					b.resize(min_len, 0);
					b.get_mut()[..last_offset].copy_from_slice(data);
				});
			}
		}

		Ok(data.len())
	}

	/// Zero out a range of data.
	///
	/// This is more efficient than [`Tree::write`] for clearing large regions.
	pub async fn write_zeros(&self, offset: u64, len: u64) -> Result<u64, Error<D>> {
		trace!("write_zeros id {}, offset {}, len {}", self.id, offset, len,);

		// Since a very large range of the object may need to be zeroed simply inserting leaf
		// records is not an option.
		//
		// To circumvent this, if a zero record is encountered, the loop goes up one level.
		// When a non-zero record is encountered, the loop goes down one level.
		// This allows skipping large amount of leaves quickly.

		if len == 0 {
			return Ok(0);
		}

		let (root, root_len) = self.root().await?;

		// Don't even bother if offset exceeds length.
		if offset >= root_len {
			return Ok(0);
		}
		let root_depth = depth(self.max_record_size(), root_len);

		// Restrict offset + len to the end of the object.
		let len = len.min(root_len - offset);

		// Zero out records from left to right
		let end = offset + len - 1;

		// Trim leftmost record
		let left_offset = offset >> self.max_record_size();
		let right_offset = (offset + len - 1) >> self.max_record_size();
		let left_trim = usize::try_from(offset % (1u64 << self.max_record_size())).unwrap();
		let right_trim =
			usize::try_from((offset + len - 1) % (1u64 << self.max_record_size())).unwrap();
		self.get(0, left_offset)
			.await?
			.modify(self.background, |data| {
				if left_offset == right_offset && right_trim < data.len() {
					// We have to trim a single record left & right.
					data.get_mut()[left_trim..=right_trim].fill(0);
				} else {
					// We have to trim the leftmost record only on the left.
					data.resize(left_trim, 0);
				}
			});

		// Completely zero records not at edges.
		let mut depth = 1;
		let mut offset = left_offset + 1;
		'z: while offset <= end >> self.max_record_size() {
			// Go up while records are zero
			loop {
				// Check if either the entry is empty or there are dirty records to inspect.
				let offt = offset >> self.cache.entries_per_parent_p2() * depth;
				let (obj, _) = self.cache.fetch_object(self.background, self.id).await?;
				let dirty = obj.data.data[usize::from(depth)]
					.dirty_markers
					.contains_key(&offt);
				drop(obj);
				// Check if either the entry is empty or there are dirty records to inspect.
				let entry = self.get(depth, offt).await?;
				if entry.len() > 0 || dirty {
					break;
				}
				depth += 1;
				if depth >= root_depth {
					break 'z;
				}
			}
			// Go down to find non-zero child.
			while depth > 1 {
				let shift_parent = self.cache.entries_per_parent_p2() * depth;
				let shift_child = self.cache.entries_per_parent_p2() * (depth - 1);
				let og_offset = offset >> shift_parent;
				for _ in 0.. {
					let (obj, _) = self.cache.fetch_object(self.background, self.id).await?;
					let dirty = obj.data.data[usize::from(depth - 1)]
						.dirty_markers
						.contains_key(&(offset >> shift_child));
					drop(obj);

					let record = {
						let key = Key::new(0, self.id, depth, offset >> shift_parent);
						if key.offset() > og_offset {
							continue 'z;
						}
						let entry = self.cache.get_entry(key).expect("no entry");
						let i = usize::try_from(
							(offset >> shift_child) % (1 << self.cache.entries_per_parent_p2()),
						)
						.unwrap();
						get_record(entry.get(), i).unwrap_or_default()
					};

					if record.length == 0 && !dirty {
						offset += 1u64 << shift_child;
					} else {
						depth -= 1;
						let key = Key::new(0, self.id, depth, offset >> shift_child);
						self.cache
							.fetch_entry(self.background, key, &record)
							.await?;
						break;
					}
				}
			}
			// Destroy leaf records & replace with zeros
			let key = Key::new(0, self.id, 1, offset >> self.cache.entries_per_parent_p2());
			let entries_per_rec = 1 << self.cache.entries_per_parent_p2();
			let mask = entries_per_rec - 1;
			for i in offset % entries_per_rec..entries_per_rec {
				let i = usize::try_from(i).unwrap();
				let entry = self.cache.get_entry(key).expect("no entry");

				let record = get_record(entry.get(), i).unwrap_or_default();
				let k = Key::new(0, self.id, 0, (offset & !mask) + u64::try_from(i).unwrap());

				drop(entry);

				let entry = self
					.cache
					.fetch_entry(self.background, k, &Record::default())
					.await?;
				entry.modify(self.background, |data| data.resize(0, 0));
			}

			// FIXME we're clearing way too much if offset + len < new_len
			let entry = self.cache.get_entry(key).expect("no entry");
			entry.modify(self.background, |data| {
				let start = usize::try_from(offset % entries_per_rec).unwrap();
				data.resize(start * mem::size_of::<Record>(), 0);
			});
			offset = (offset + mask + 1) & !mask;
		}

		Ok(len)
	}

	/// Read data from a range.
	///
	/// Returns the actual amount of bytes read.
	/// It may exit early if not all data is cached.
	pub async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
		trace!("read id {}, offset {}, len {}", self.id, offset, buf.len());

		let (root, root_len) = self.root().await?;

		// Ensure all data fits in buffer.
		let buf = if root_len <= offset {
			return Ok(0);
		} else if offset.saturating_add(u64::try_from(buf.len()).unwrap()) >= root_len {
			&mut buf[..usize::try_from(root_len - offset).unwrap()]
		} else {
			buf
		};

		if buf.is_empty() {
			return Ok(0);
		}

		let range = calc_range(self.max_record_size(), offset, buf.len());
		let (first_offset, last_offset) =
			calc_record_offsets(self.max_record_size(), offset, buf.len());

		/// Copy from record to first half of `buf` and fill remainder with zeroes.
		#[track_caller]
		fn copy(buf: &mut [u8], data: &[u8]) {
			buf[..data.len()].copy_from_slice(data);
			buf[data.len()..].fill(0);
		}

		let buf_len = buf.len();

		if range.start() == range.end() {
			// We need to slice one record twice
			let b = self.get(0, *range.start()).await?;

			let b = b.get().get(first_offset..).unwrap_or(&[]);
			copy(buf, &b[..buf.len().min(b.len())]);
		} else {
			// We need to slice the first & last record once and operate on the others in full.
			let mut buf = buf;
			let mut range = range.into_iter();

			let first_key = range.next().unwrap();
			let last_key = range.next_back().unwrap();

			// Copy to first record |----xxxx|
			{
				let b;
				(b, buf) = buf.split_at_mut((1usize << self.max_record_size()) - first_offset);
				let d = self.get(0, first_key).await?;
				copy(b, d.get().get(first_offset..).unwrap_or(&[]));
			}

			// Copy middle records |xxxxxxxx|
			for key in range {
				let b;
				(b, buf) = buf.split_at_mut(1usize << self.max_record_size());
				let d = self.get(0, key).await?;
				copy(b, d.get());
			}

			// Copy end record |xxxx----|
			// Don't bother if there's nothing to copy
			if last_offset > 0 {
				debug_assert_eq!(buf.len(), last_offset);
				let d = self.get(0, last_key).await?;
				let max_len = d.len().min(buf.len());
				copy(buf, &d.get()[..max_len]);
			}
		}

		Ok(buf_len)
	}

	/// Update a record.
	/// This will write the record to the parent record or the root of this object.
	// TODO avoid Box
	#[async_recursion::async_recursion(?Send)]
	pub(super) async fn update_record(
		&self,
		record_depth: u8,
		offset: u64,
		record: Record,
	) -> Result<(), Error<D>> {
		trace!(
			"update_record id {}, depth {}, offset {}, record.(lba, length) ({}, {})",
			self.id,
			record_depth,
			offset,
			record.lba,
			record.length
		);
		// The object is guaranteed to exist.
		// At least, if called by a task that holds an entry, which should be guaranteed.
		let cur_root = self.cache.get_object(self.id).expect("no object").root;
		let len = u64::from(cur_root.total_length);
		let cur_depth = depth(self.max_record_size(), len);
		let parent_depth = record_depth + 1;
		assert!(parent_depth <= cur_depth);

		if cur_root.length == 0 && record.length == 0 {
			// Both the record and root are zero, so don't dirtying the parent.
			trace!("--> skip record & root zero");
			return Ok(());
		}

		if cur_depth == parent_depth {
			assert_eq!(offset, 0, "root can only be at offset 0");

			// Copy total length and references to new root.
			let new_root =
				Record { total_length: len.into(), references: cur_root.references, ..record };
			trace!(
				"--> replace root ({}, {}) -> ({}, {})",
				new_root.lba,
				new_root.length,
				cur_root.lba,
				cur_root.length
			);

			// Destroy old root.
			self.cache.store.destroy(&cur_root);

			// Store new root
			// The object is guaranteed to be in the cache as update_record is only called
			// during flush or evict.
			let mut obj = self.cache.get_object(self.id).expect("no object");
			obj.root = new_root;
			obj.set_dirty(true);
		} else {
			// Update a parent record.
			// Find parent
			let shift = self.max_record_size().to_raw() - RECORD_SIZE_P2;
			let (offt, index) = divmod_p2(offset, shift);

			let k = Key::new(0, self.id, parent_depth, offt);
			let entry = self.cache.tree_fetch_entry(self.background, k).await?;
			let old_record = get_record(entry.get(), index).unwrap_or_default();
			if old_record.length == 0 && record.length == 0 {
				// Both the old and new record are zero, so don't dirty the parent.
				trace!("--> skip both zero");
				return Ok(());
			}
			entry.modify(self.background, |data| {
				// Destroy old record
				trace!(
					"--> replace parent ({}, {}) -> ({}, {})",
					record.lba,
					record.length,
					old_record.lba,
					old_record.length
				);
				self.cache.store.destroy(&old_record);

				// Calc offset in parent
				let index = index * mem::size_of::<Record>();
				let min_len = data.len().max(index + mem::size_of::<Record>());

				// Store new record
				data.resize(min_len, 0);
				data.get_mut()[index..index + mem::size_of::<Record>()]
					.copy_from_slice(record.as_ref());
			})
		}
		Ok(())
	}

	/// Resize record tree.
	pub async fn resize(&self, new_len: u64) -> Result<(), Error<D>> {
		trace!("resize id {} new_len {}", self.id, new_len);
		let (root, len) = self.root().await?;
		if new_len < len {
			self.shrink(new_len, &root).await
		} else if new_len > len {
			self.grow(new_len, &root).await
		} else {
			Ok(())
		}
	}

	/// Shrink record tree.
	async fn shrink(&self, new_len: u64, &cur_root: &Record) -> Result<(), Error<D>> {
		trace!("shrink id {}, new_len {}", self.id, new_len);
		// Steps:
		// 1. If the depth changes, find new root.
		// 1a. Split object into two: current with new root and pseudo-object with records to
		//     be zeroed.
		// 2. Zero out data past new_len.

		let cur_len = u64::from(cur_root.total_length);

		debug_assert!(
			new_len < cur_len,
			"new len is equal or greater than cur len"
		);

		let cur_depth = depth(self.max_record_size(), cur_len);
		let new_depth = depth(self.max_record_size(), new_len);

		if new_depth < cur_depth {
			// 1. If the depth changes, find new root.
			let new_root = if new_depth == 0 {
				Record::default()
			} else {
				// Take new root out
				let parent = self.get(new_depth, 0).await?;
				let rec = get_record(parent.get(), 0).unwrap_or_default();
				parent.modify(self.background, |d| {
					if d.len() > 32 {
						d.get_mut()[..32].fill(0)
					} else {
						d.resize(0, 0)
					}
				});
				rec
			};
			let new_root = Record {
				references: cur_root.references,
				total_length: new_len.into(),
				..new_root
			};

			// 1a. Split object into two: current with new root and pseudo-object with records to
			//     be zeroed.
			let mut data = self.cache.data.borrow_mut();
			let pseudo_id = data.new_pseudo_id();
			let Some(Slot::Present(cur_obj)) = data.objects.get_mut(&self.id)
				else { unreachable!("no object") };
			let mut pseudo_obj = data::TreeData::new(new_root, self.max_record_size());

			// Transfer all entries with offset < new_len to pseudo object
			let mut offt = 1 << (self.max_record_size().to_raw() * new_depth);
			let mut refcount = 0;
			for d in 0..new_depth {
				// Move entries
				let cur_level = &mut cur_obj.data.data[usize::from(d)];
				let pseudo_level = &mut pseudo_obj.data[usize::from(d)];
				for (offset, slot) in cur_level.slots.drain_filter(|k, _| *k >= offt) {
					pseudo_level.slots.insert(offset, slot);
					if let Some(marker) = cur_level.dirty_markers.remove(&offset) {
						pseudo_level.dirty_markers.insert(offset, marker);
					}
				}
				refcount += cur_level.slots.len();
				offt >>= self.max_record_size().to_raw();
			}
			for d in new_depth..cur_depth {
				let cur_level = &mut cur_obj.data.data[usize::from(d)];
				refcount += cur_level.slots.len();
			}
			// Fix marker count for cur_obj
			cur_obj.data.data[usize::from(new_depth)]
				.dirty_markers
				.remove(&0);

			// Swap & insert pseudo-object.
			// If the pseudo-object has no entries, it's already zeroed and there is nothing
			// left to do.
			mem::swap(&mut cur_obj.data, &mut pseudo_obj);
			if let Some(count) = NonZeroUsize::new(refcount) {
				// Fix keys of any busy tasks
				for lvl in pseudo_obj.data.iter_mut() {
					for slot in lvl.slots.values() {
						if let Slot::Busy(busy) = &slot {
							let mut busy = busy.borrow_mut();
							busy.key = Key::new(
								busy.key.flags(),
								pseudo_id,
								busy.key.depth(),
								busy.key.offset(),
							);
						}
					}
				}

				let refcount = RefCount::Ref { count };
				let present = Present { data: pseudo_obj, refcount };
				data.objects.insert(pseudo_id, Slot::Present(present));
				// Zero out pseudo-object.
				drop(data);
				Tree::new(self.cache, self.background, pseudo_id)
					.write_zeros(0, u64::MAX)
					.await?;
			}
		}

		// Zero out data written past the end.
		self.write_zeros(new_len, u64::MAX).await?;

		Ok(())
	}

	/// Grow record tree.
	async fn grow(&self, new_len: u64, &cur_root: &Record) -> Result<(), Error<D>> {
		trace!("grow id {}, new_len {}", self.id, new_len);
		// There are two cases to consider when growing a record tree:
		//
		// * The depth does not change.
		//   Nothing to do then.
		//
		// * The depth changes.
		//   *Move* the root record to a new record and zero out the root record entry.
		//   The dirty new record will bubble up and eventually a new root entry is created.
		//
		// Steps:
		// 1. Adjust levels array size to new_depth.
		// 2. Propagate dirty counters up.
		// 3. Insert parent right above current root record if depth changes.
		//    Do this without await!
		// 4. Update root record.
		//    If depth changes, make it a zero record.
		//    Otherwise, copy cur_root.

		let cur_len = u64::from(cur_root.total_length);

		debug_assert!(
			cur_len < new_len,
			"new len is equal or smaller than cur len"
		);

		let cur_depth = depth(self.max_record_size(), cur_len);
		let new_depth = depth(self.max_record_size(), new_len);

		// The object is guaranteed to be in the cache since we have cur_root.
		let mut data_ref = self.cache.data.borrow_mut();
		let data = &mut *data_ref;
		let Some(Slot::Present(obj)) = data.objects.get_mut(&self.id)
			else { unreachable!("no object") };

		// 1. Adjust levels array size to new_depth
		let mut v = mem::take(&mut obj.data.data).into_vec();
		debug_assert_eq!(v.len(), usize::from(cur_depth));
		v.resize_with(new_depth.into(), Default::default);
		obj.data.data = v.into();

		// Check if the depth changed.
		// If so we need to move the current root.
		if cur_depth < new_depth {
			// 2. Propagate dirty counters up.
			// This simply involves inserting counters at 0 offsets.
			// Since we're going to insert a dirty entry, do it unconditionally.
			// Mark first level as dirty.
			let dirty_descendant = cur_depth > 0
				&& obj.data.data[usize::from(cur_depth - 1)]
					.dirty_markers
					.contains_key(&0);
			let [level, levels @ ..] = &mut obj.data.data[usize::from(cur_depth)..]
				else { unreachable!("out of range") };
			let marker = level.dirty_markers.entry(0).or_default();
			marker.is_dirty = true;
			if dirty_descendant {
				marker.children.insert(0);
			}
			// Mark other levels as having a dirty child.
			for lvl in levels.iter_mut() {
				lvl.dirty_markers.entry(0).or_default().children.insert(0);
			}

			// 3. Insert parent right above current root record if depth changes.
			//    Do this without await!
			let key = Key::new(0, self.id, cur_depth, 0);
			let mut d = self.cache.resource().alloc();
			d.resize(core::mem::size_of::<Record>(), 0);
			d.get_mut().copy_from_slice(
				Record { total_length: 0.into(), references: 0.into(), ..cur_root }.as_ref(),
			);
			let lru_index = data.lru.add(key, CACHE_ENTRY_FIXED_COST + d.len());
			let entry = Slot::Present(Present { data: d, refcount: RefCount::NoRef { lru_index } });
			obj.add_entry(&mut data.lru, key.depth(), key.offset(), entry);

			// 4. Update root record.
			//    If depth changes, make it a zero record.
			obj.data.root = Record {
				total_length: new_len.into(),
				references: cur_root.references,
				..Default::default()
			};

			// We just added a record, so evict excess.
			drop(data_ref);
			self.cache.evict_excess(self.background);
		} else {
			// Just adjust length and presto
			obj.data.root = Record { total_length: new_len.into(), ..obj.data.root }
		}

		Ok(())
	}

	/// The length of the record tree in bytes.
	pub async fn len(&self) -> Result<u64, Error<D>> {
		trace!("len id {}", self.id);
		self.root().await.map(|(_, len)| len)
	}

	/// Replace the data of this object with the data of another object.
	///
	/// The other object is destroyed.
	///
	/// # Panics
	///
	/// There is more than one active lock on the other object,
	/// i.e. there are multiple [`Tree`] instances referring to the same object.
	/// Hence the object cannot safely be destroyed.
	pub async fn replace_with(&self, other: Tree<'a, 'b, D, R>) -> Result<(), Error<D>> {
		// FIXME check locks
		self.cache
			.move_object(self.background, other.id, self.id)
			.await
	}

	/// Increase the reference count of an object.
	///
	/// This may fail if the reference count is already [`u16::MAX`].
	/// On failure, the returned value is `false`, otherwise `true`.
	pub async fn increase_reference_count(&self) -> Result<bool, Error<D>> {
		debug_assert_ne!(
			self.id, OBJECT_LIST_ID,
			"object list isn't reference counted"
		);

		let (mut obj, _) = self.cache.fetch_object(self.background, self.id).await?;
		debug_assert!(obj.data.root.references != 0, "invalid object");
		if obj.data.root.references == u16::MAX {
			return Ok(false);
		}
		obj.data.root.references += 1;

		Ok(true)
	}

	/// Decrease the reference count of an object.
	///
	/// If the reference count reaches 0 the object is destroyed
	/// and the tree should not be used anymore.
	pub async fn decrease_reference_count(&self) -> Result<(), Error<D>> {
		debug_assert_ne!(
			self.id, OBJECT_LIST_ID,
			"object list isn't reference counted"
		);

		let (mut obj, _) = self.cache.fetch_object(self.background, self.id).await?;
		debug_assert!(obj.data.root.references != 0, "invalid object");
		obj.data.root.references -= 1;

		if obj.data.root.references == 0 {
			drop(obj);
			// Free space.
			self.resize(0).await?;
			self.cache.data.borrow_mut().dealloc_id(self.id);
		}

		Ok(())
	}

	/// Get the maximum record size.
	fn max_record_size(&self) -> MaxRecordSize {
		self.cache.max_record_size()
	}

	/// Get the ID of this object.
	pub fn id(&self) -> u64 {
		self.id
	}

	/// Get the root and length of this tree.
	async fn root(&self) -> Result<(Record, u64), Error<D>> {
		let (obj, _) = self.cache.fetch_object(self.background, self.id).await?;
		Ok((obj.data.root, u64::from(obj.data.root.total_length)))
	}
}

/// Determine record range given an offset, record size and length.
///
/// Ranges are used for efficient iteration.
fn calc_range(record_size: MaxRecordSize, offset: u64, length: usize) -> RangeInclusive<u64> {
	let start_key = offset >> record_size;
	let end_key = (offset + u64::try_from(length).unwrap()) >> record_size;
	start_key..=end_key
}

/// Determine start & end offsets inside records.
fn calc_record_offsets(record_size: MaxRecordSize, offset: u64, length: usize) -> (usize, usize) {
	let mask = (1u64 << record_size) - 1;
	let start = offset & mask;
	let end = (offset + u64::try_from(length).unwrap()) & mask;
	(start.try_into().unwrap(), end.try_into().unwrap())
}

/// Calculate divmod with a power of two.
fn divmod_p2(offset: u64, pow2: u8) -> (u64, usize) {
	let mask = (1u64 << pow2) - 1;

	let index = offset & mask;
	let offt = offset >> pow2;

	(offt, index.try_into().unwrap())
}

/// Calculate depth given record size and total length.
pub(super) fn depth(max_record_size: MaxRecordSize, len: u64) -> u8 {
	if len == 0 {
		0
	} else {
		let mut depth = 0;
		// max len = maximum amount of bytes record tree can hold at current depth minus 1
		let mut max_len_mask = (1u64 << max_record_size) - 1;
		let len_mask = len - 1;
		while len_mask > max_len_mask {
			depth += 1;
			max_len_mask |= max_len_mask << max_record_size.to_raw() - RECORD_SIZE_P2;
		}
		depth + 1
	}
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn depth_rec1k_len0() {
		assert_eq!(depth(MaxRecordSize::K1, 0), 0);
	}

	#[test]
	fn depth_rec1k_len1() {
		assert_eq!(depth(MaxRecordSize::K1, 1), 1);
	}

	#[test]
	fn depth_rec1k_len1023() {
		assert_eq!(depth(MaxRecordSize::K1, 1023), 1);
	}

	#[test]
	fn depth_rec1k_len1024() {
		assert_eq!(depth(MaxRecordSize::K1, 1024), 1);
	}

	#[test]
	fn depth_rec1k_len1025() {
		assert_eq!(depth(MaxRecordSize::K1, 1025), 2);
	}

	#[test]
	fn depth_rec1k_2p10() {
		assert_eq!(depth(MaxRecordSize::K1, 1 << 10 + 5 * 0), 1);
	}

	#[test]
	fn depth_rec1k_2p15() {
		assert_eq!(depth(MaxRecordSize::K1, 1 << 10 + 5 * 1), 2);
	}

	#[test]
	fn depth_rec1k_2p20() {
		assert_eq!(depth(MaxRecordSize::K1, 1 << 10 + 5 * 2), 3);
	}

	#[test]
	fn depth_rec1k_2p20_plus_1() {
		assert_eq!(depth(MaxRecordSize::K1, (1 << 10 + 5 * 2) + 1), 4);
	}

	#[test]
	fn depth_rec1k_2p40() {
		assert_eq!(depth(MaxRecordSize::K1, 1 << 10 + 5 * 6), 7);
	}

	#[test]
	fn depth_rec1k_lenmax() {
		assert_eq!(depth(MaxRecordSize::K1, u64::MAX), 12);
	}
}
