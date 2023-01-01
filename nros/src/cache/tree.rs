use {
	super::{Cache, EntryRef, Key, TreeData, OBJECT_LIST_ID},
	crate::{util::get_record, Dev, Error, MaxRecordSize, Record},
	core::{cell::RefMut, future, mem, ops::RangeInclusive, task::Poll},
};

const RECORD_SIZE_P2: u8 = mem::size_of::<Record>().ilog2() as _;

/// Implementation of a record tree.
///
/// As long as a `Tree` object for a specific ID is alive its [`TreeData`] entry will not be
/// evicted.
// FIXME guarantee TreeData will not be evicted (with locks?).
#[derive(Clone, Debug)]
pub struct Tree<'a, D: Dev> {
	/// Underlying cache.
	cache: &'a Cache<D>,
	/// ID of the object.
	id: u64,
}

impl<'a, D: Dev> Tree<'a, D> {
	/// Access a tree.
	pub(super) async fn new(cache: &'a Cache<D>, id: u64) -> Result<Tree<'a, D>, Error<D>> {
		// Alas, async recursion is hard
		if id == OBJECT_LIST_ID {
			Self::new_object_list(cache).await
		} else {
			Self::new_object(cache, id).await
		}
	}

	/// Access an object.
	///
	/// # Panics
	///
	/// If `id == OBJECT_LIST_ID`.
	pub(super) async fn new_object(cache: &'a Cache<D>, id: u64) -> Result<Tree<'a, D>, Error<D>> {
		assert!(id != OBJECT_LIST_ID);
		Ok(Self { cache, id })
	}

	/// Access the object list.
	pub(super) async fn new_object_list(cache: &'a Cache<D>) -> Result<Tree<'a, D>, Error<D>> {
		let id = OBJECT_LIST_ID;
		Ok(Self { cache, id })
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

		let (_lock, root) = self.cache.lock_readwrite(self.id).await?;
		let len = u64::from(root.total_length);
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
			entry
				.modify(|b| {
					let min_len = last_offset.max(b.len());
					b.resize(min_len, 0);
					b[first_offset..last_offset].copy_from_slice(data);
				})
				.await?;
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
				entry
					.modify(|b| {
						b.resize(first_offset, 0);
						b.extend_from_slice(d);
					})
					.await?;
			}

			// Copy middle records |xxxxxxxx|
			for offset in range {
				let d;
				(d, data) = data.split_at(1usize << self.max_record_size());

				// "Fetch" directly since we're overwriting the entire record anyways.
				let key = Key::new(self.id, 0, offset);
				let entry = self
					.cache
					.fetch_entry(key, &Record::default(), max_depth)
					.await?;

				entry
					.modify(|b| {
						// If the record was already fetched, it'll have ignored the &Record::default().
						// Hence we need to clear it manually.
						b.clear();
						b.extend_from_slice(d);
					})
					.await?;
			}

			// Copy end record |xxxx----|
			// Don't bother if there is no data
			if last_offset > 0 {
				debug_assert_eq!(data.len(), last_offset);
				let entry = self.get(0, last_key).await?;
				entry
					.modify(|b| {
						let min_len = b.len().max(data.len());
						b.resize(min_len, 0);
						b[..last_offset].copy_from_slice(data);
					})
					.await?;
			}
		}

		Ok(data.len())
	}

	/// Zero out a range of data.
	///
	/// This is more efficient than [`Tree::write`] for clearing large regions.
	pub async fn write_zeros(&self, offset: u64, len: u64) -> Result<u64, Error<D>> {
		trace!("write_zeros id {}, offset {}, len {}", self.id, offset, len,);
		dbg!(&self.cache.data);

		// Since a very large range of the object may need to be zeroed simply inserting leaf
		// records is not an option.
		//
		// To circumvent this, if a zero record is encountered, the loop goes up one level.
		// When a non-zero record is encountered, the loop goes down one level.
		// This allows skipping large amount of leaves quickly.

		if len == 0 {
			return Ok(0);
		}

		let (_lock, root) = self.cache.lock_readwrite(self.id).await?;

		// Don't even bother if offset exceeds length.
		let root_len = u64::from(root.total_length);
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
			.modify(|data| {
				if left_offset == right_offset && right_trim < data.len() {
					// We have to trim a single record left & right.
					data[left_trim..=right_trim].fill(0);
				} else {
					// We have to trim the leftmost record only on the left.
					data.resize(left_trim, 0);
				}
			})
			.await?;

		// Completely zero records not at edges.
		let mut depth = 1;
		let mut offset = left_offset + 1;
		dbg!(end);
		'z: while offset <= end >> self.max_record_size() {
			dbg!(offset);
			// Go up while records are zero
			loop {
				dbg!(depth);
				// Check if either the entry is empty or there are dirty records to inspect.
				let offt = offset >> (self.max_record_size().to_raw() - RECORD_SIZE_P2) * depth;
				let dirty = self.cache.get_object_entry_mut(self.id, root_depth).data
					[usize::from(depth)]
				.dirty_counters
				.contains_key(&offt);
				// Check if either the entry is empty or there are dirty records to inspect.
				let entry = self.get(depth, offt).await?;
				if !entry.data.is_empty() || dirty {
					break;
				}
				depth += 1;
				if depth >= root_depth {
					break 'z;
				}
			}
			// Go down to find non-zero child.
			while depth > 1 {
				dbg!(depth);
				let shift = (self.max_record_size().to_raw() - RECORD_SIZE_P2) * depth;
				let og_offset = offset >> shift;
				for i in 0.. {
					let shift = (self.max_record_size().to_raw() - RECORD_SIZE_P2) * (depth - 1);
					let dirty = self.cache.get_object_entry_mut(self.id, root_depth).data
						[usize::from(depth - 1)]
					.dirty_counters
					.contains_key(&(offset >> shift));

					let record = {
						let shift = (self.max_record_size().to_raw() - RECORD_SIZE_P2) * depth;
						let key = Key::new(self.id, depth, offset >> shift);
						if key.offset() > og_offset {
							continue 'z;
						}
						let entry = self.cache.get_entry(key).expect("no entry");
						get_record(&entry.data, i).unwrap_or_default()
					};

					if record.length == 0 && !dirty {
						offset += 1u64 << shift;
					} else {
						depth -= 1;
						let key = Key::new(self.id, depth, offset >> shift);
						self.cache.fetch_entry(key, &record, depth).await?;
						break;
					}
				}
			}
			// Destroy leaf records & replace with zeros
			let key = Key::new(
				self.id,
				1,
				offset >> self.max_record_size().to_raw() - RECORD_SIZE_P2,
			);
			let entries_per_rec = 1 << self.max_record_size().to_raw() - RECORD_SIZE_P2;
			let mask = entries_per_rec - 1;
			dbg!(offset, entries_per_rec);
			for i in offset / entries_per_rec % entries_per_rec..entries_per_rec {
				let i = usize::try_from(i).unwrap();
				let entry = self.cache.get_entry(key).expect("no entry");

				let record = get_record(&entry.data, i).unwrap_or_default();
				let k = Key::new(
					self.id,
					0,
					(offset & !mask) + u64::try_from(i).unwrap(),
				);

				drop(entry);
				self.cache.destroy_entry(k, &record);
			}
			let entry = self.cache.get_entry(key).expect("no entry");
			entry.modify(|data| data.clear()).await?;
			dbg!(offset);
			offset = (offset + mask + 1) & !mask;
			dbg!(offset);
		}

		Ok(len)
	}

	/// Read data from a range.
	///
	/// Returns the actual amount of bytes read.
	/// It may exit early if not all data is cached.
	pub async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
		trace!("read id {}, offset {}, len {}", self.id, offset, buf.len());

		let (_lock, root) = self.cache.lock_readwrite(self.id).await?;
		let len = u64::from(root.total_length);

		// Ensure all data fits in buffer.
		let buf = if len <= offset {
			return Ok(0);
		} else if offset.saturating_add(u64::try_from(buf.len()).unwrap()) >= len {
			&mut buf[..usize::try_from(len - offset).unwrap()]
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

			let b = b.data.get(first_offset..).unwrap_or(&[]);
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
				copy(b, d.data.get(first_offset..).unwrap_or(&[]));
			}

			// Copy middle records |xxxxxxxx|
			for key in range {
				let b;
				(b, buf) = buf.split_at_mut(1usize << self.max_record_size());
				let d = self.get(0, key).await?;
				copy(b, &d.data);
			}

			// Copy end record |xxxx----|
			// Don't bother if there's nothing to copy
			if last_offset > 0 {
				debug_assert_eq!(buf.len(), last_offset);
				let d = self.get(0, last_key).await?;
				let max_len = d.data.len().min(buf.len());
				copy(buf, &d.data[..max_len]);
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
		let (cur_root, len) = self.root().await?;
		let cur_depth = depth(self.max_record_size(), len);
		let parent_depth = record_depth + 1;
		assert!(parent_depth <= cur_depth);
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
			if self.id == OBJECT_LIST_ID {
				self.cache.store.set_object_list(new_root);
				Ok(())
			} else {
				self.cache.set_object_root(self.id, &new_root).await
			}
		} else {
			// Update a parent record.
			// Find parent
			let shift = self.max_record_size().to_raw() - RECORD_SIZE_P2;
			let (offt, index) = divmod_p2(offset, shift);

			let entry = self.get(parent_depth, offt).await?;
			entry
				.modify(|data| {
					// Destroy old record
					let old_record = get_record(data, index).unwrap_or_default();
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
					data[index..index + mem::size_of::<Record>()].copy_from_slice(record.as_ref());
				})
				.await
		}
	}

	/// Resize record tree.
	pub async fn resize(&self, new_len: u64) -> Result<(), Error<D>> {
		trace!("resize id {} new_len {}", self.id, new_len);
		let _lock = self.cache.lock_resizing(self.id, new_len).await;
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
		todo!();
		/*
		// Shrinking is one of the trickiest operations to implement properly.
		// Things to take care of are:
		//
		// * Ensuring no other (concurrent) resizes happen on the same tree.
		// * Limiting read & writes length, so they don't read or write data past new_len
		// * Stay within cache memory limits.
		//
		// The following procedure is used:
		//
		// 1. Replace root.
		//    This requires a brief lock to ensure no reads & writes are in progress.
		//    When the root is replaced it is guaranteed out-of-range records will
		//    no longer be accessed outside shrink.
		//    To find the new root, go down the leftmost subtree until the new depth is reached.
		// 2. Remove out-of-range leaves from cache.
		//    Doing this early on can save on redundant flushes.
		// 3. Remove above-root records.
		//    That is, for every level >= new_depth, remove all but the leftmost subtree of each
		//    parent, then the parent itself, repeat until new_depth is reached.
		//    a. The case of new_depth == 0 must be handled specially as a leaf must not be treated
		//       like a parent record.
		//       In this case, the remaining root just needs to be destroyed.
		// 4. Adjust `TreeData` levels array size, if present.
		// 5. Trim parent records.
		//    For every level, figure out the max valid offset given new_length and the depth.
		//    Then destroy every subtree strictly to the right.
		//    The rightmost valid record then may need to be shrunk.
		//    If it is a parent, destroy the removed subtrees.
		// 6. Trim rightmost leaf.

		/// Destroy all records in a subtree recursively.
		async fn destroy<D: Dev>(
			cache: &Cache<D>,
			key: Key,
			record: &Record,
			max_depth: u8,
		) -> Result<(), Error<D>> {
			trace!(
				"shrink::destroy key {:?} record.(lba, length) ({}, {})",
				key,
				record.lba,
				record.length,
			);

			// Check if this record has been substituted with a record present in the lock cache
			let mut data = cache.data.borrow_mut();
			let lock = data.resizing.get_mut(&key.id()).expect("no resizing lock");
			let record = lock
				.destroy_records
				.remove(&(key.depth(), key.offset()))
				.unwrap_or(*record);
			drop(data);

			// The actually recursive part, which does require boxing the future
			#[async_recursion::async_recursion(?Send)]
			async fn f<D: Dev>(
				cache: &Cache<D>,
				key: Key,
				record: &Record,
				max_depth: u8,
			) -> Result<(), Error<D>> {
				// We're a parent node, destroy children.
				let records_per_parent_p2 = cache.max_record_size().to_raw() - RECORD_SIZE_P2;
				let mut record = *record;
				for index in 0.. {
					let entry;
					(entry, record) = fetch_entry(cache, key, &record, max_depth).await?;
					let Some(rec) = get_record(&entry.data, index) else { break };
					drop(entry);
					let offt =
						(key.offset() << records_per_parent_p2) + u64::try_from(index).unwrap();
					let k = Key::new(key.id(), key.depth() - 1, offt);
					destroy(cache, k, &rec, max_depth).await?;
				}
				Ok(())
			}

			if key.depth() > 0 && record.length > 0 {
				f(cache, key, &record, max_depth).await?
			}

			cache.destroy_entry(key, &record);

			Ok(())
		}

		/// Fetch an entry.
		///
		/// Unlike [`Cache::fetch_entry`], this also checks `CacheData::resizing` for out-of-range
		/// entries that have been flushed.
		///
		/// Returns the record, which may be different if it was indeed flushed.
		async fn fetch_entry<'a, D: Dev>(
			cache: &'a Cache<D>,
			key: Key,
			record: &Record,
			max_depth: u8,
		) -> Result<(EntryRef<'a, D>, Record), Error<D>> {
			trace!("shrink::fetch_entry {:?} <- {:?}", key, record.lba);
			// Check lock first for new records.
			let mut data = cache.data.borrow_mut();
			let lock = data.resizing.get_mut(&key.id()).expect("no resizing lock");
			let record = lock
				.destroy_records
				.remove(&(key.depth(), key.offset()))
				.unwrap_or(*record);
			drop(data);
			// Fetch
			cache
				.fetch_entry(key, &record, max_depth)
				.await
				.map(|e| (e, record))
		}

		/// Get a leftmost entry at a specific depth.
		///
		/// Unlike [`Tree::get`], this also checks `CacheData::resizing` for out-of-range
		/// entries that have been flushed.
		async fn get_left<'a, D: Dev>(
			cache: &'a Cache<D>,
			root: &Record,
			key: Key,
			max_depth: u8,
		) -> Result<EntryRef<'a, D>, Error<D>> {
			trace!("shrink::get_left {:?}", key);
			// Find first ancestor of new root (or the root itself)
			let data = cache.data.borrow_mut();
			let (mut obj, lrus) = RefMut::map_split(data, |d| {
				(
					d.data
						.entry(key.id())
						.or_insert_with(|| TreeData::new(max_depth)),
					&mut d.lrus,
				)
			});

			let (mut depth, mut record) = (max_depth - 1, *root);
			for d in key.depth()..max_depth {
				match RefMut::filter_map(obj, |obj| obj.data[usize::from(d)].get_mut(&0)) {
					Ok(entry) => {
						if d == key.depth() {
							return Ok(EntryRef { cache, entry, key, lrus });
						}
						record = get_record(&entry.data, 0).unwrap_or_default();
						depth = d - 1;
						break;
					}
					Err(o) => obj = o,
				}
			}
			// Recurse downwards
			debug_assert!(depth < max_depth);
			debug_assert!(depth >= key.depth());
			loop {
				let k = Key::new(key.id(), depth, 0);
				let (entry, _) = fetch_entry(cache, k, &record, max_depth).await?;
				if depth == key.depth() {
					return Ok(entry);
				}
				record = get_record(&entry.data, 0).unwrap_or_default();
				depth -= 1;
			}
		}

		let rec_size_p2 = self.max_record_size().to_raw();

		let cur_len = u64::from(cur_root.total_length);

		let cur_depth = depth(self.max_record_size(), cur_len);
		let new_depth = depth(self.max_record_size(), new_len);

		debug_assert!(cur_len > new_len, "new_len is equal or larger than cur len");
		dbg!(cur_depth, new_depth);

		// 1. Replace root
		let (cur_root, new_root) = {
			// Lock to prevent reads & writes while replacing root.
			let (_lock, cur_root) = self.cache.lock_root_replace(self.id).await?;
			// Find new root.
			// Don't use Self::get as we need to check for flushed entry in the resize lock.
			let mut new_root = if new_depth == 0 {
				Record::default()
			} else if new_depth == cur_depth {
				cur_root
			} else {
				let entry = get_left(
					self.cache,
					&cur_root,
					Key::new(self.id, new_depth, 0),
					cur_depth,
				)
				.await?;
				get_record(&entry.data, 0).unwrap_or_default()
			};
			// Set total length and references
			new_root.total_length = new_len.into();
			new_root.references = cur_root.references;
			// Set root.
			self.cache.set_object_root(self.id, &new_root).await?;
			dbg!(cur_root, new_root)
		};

		// 2. Wipe out-of-range leaves.
		{
			let (mut data, mut lrus) = RefMut::map_split(self.cache.data.borrow_mut(), |data| {
				let obj = data
					.data
					.entry(self.id)
					.or_insert_with(|| TreeData::new(cur_depth));
				(&mut obj.data[0], &mut data.lrus)
			});
			if new_len > 0 {
				let max_offset = calc_offset(self.max_record_size(), new_len, 0);
				for (_, entry) in data.drain_filter(|&o, _| o > max_offset) {
					lrus.adjust_cache_removed_entry(&entry);
				}
			} else {
				for (_, entry) in data.drain() {
					lrus.adjust_cache_removed_entry(&entry);
				}
			}
		}

		// 3. Remove above-root records.
		{
			let mut root = cur_root;
			for d in (new_depth.max(1)..cur_depth).rev() {
				dbg!(&root);
				dbg!(d);
				// Destroy all subtrees except the leftmost one.
				let key = Key::new(self.id, d, 0);
				for i in 1.. {
					let entry;
					(entry, root) = fetch_entry(self.cache, key, &root, cur_depth).await?;
					let Some(record) = get_record(&entry.data, i) else { break };
					drop(entry);
					let k = Key::new(self.id, d - 1, i.try_into().unwrap());
					destroy(self.cache, k, &record, cur_depth).await?;
				}
				// Replace & destroy the root.
				let entry;
				(entry, root) = fetch_entry(self.cache, key, &root, cur_depth).await?;
				let record = get_record(&entry.data, 0).unwrap_or_default();
				drop(entry);
				self.cache.destroy_entry(key, &root);
				root = record;

				// Destroy other subtrees that may be dangling on the same level.
				// Collect offsets to guarantee O(n) runtime.
				let offsets = {
					let data = &mut *self.cache.data.borrow_mut();
					data.data
						.entry(self.id)
						.or_insert_with(|| TreeData::new(new_depth))
						.data[usize::from(d)]
					.keys()
					.copied()
					.chain(
						data.resizing
							.get(&self.id)
							.unwrap()
							.destroy_records
							.keys()
							.copied()
							.filter(|&(depth, offt)| depth == d)
							.map(|(_, offt)| offt),
					)
					.collect::<Vec<_>>()
				};

				for offset in offsets {
					let key = Key::new(self.id, d, offset);
					let mut record = None;
					// Destroy children.
					for i in 0.. {
						let entry = if let Some(e) = self.cache.get_entry(key) {
							e
						} else {
							// If it's not in the cache, it has been flushed.
							// Since it was still in the cache it *had* to be a dirty record.
							// Since it was a dirty record, a reference is stored in the resize lock.
							// Check lock first for new records.
							let rec = if let Some(r) = record {
								r
							} else {
								let mut data = self.cache.data.borrow_mut();
								let lock =
									data.resizing.get_mut(&key.id()).expect("no resizing lock");
								let rec = lock
									.destroy_records
									.remove(&(key.depth(), key.offset()))
									.expect("no entry in resize lock");
								*record.insert(rec)
							};
							self.cache.fetch_entry(key, &rec, cur_depth).await?
						};
						let Some(record) = get_record(&entry.data, i) else { break };
						drop(entry);
						let offt = (offset << self.max_record_size().to_raw() - RECORD_SIZE_P2)
							+ u64::try_from(i).unwrap();
						let k = Key::new(self.id, d - 1, offt);
						dbg!();
						destroy(self.cache, k, &record, cur_depth).await?;
						dbg!();
					}
					// Remove the entry if it is still present.
					if self.cache.remove_entry(key).is_none() {
						// If it's not in the cache, it has been flushed.
						// ditto ditto ditto
						let rec = if let Some(r) = record {
							r
						} else {
							let mut data = self.cache.data.borrow_mut();
							let lock = data.resizing.get_mut(&key.id()).expect("no resizing lock");
							let rec = lock
								.destroy_records
								.remove(&(key.depth(), key.offset()))
								.expect("no entry in resize lock");
							*record.insert(rec)
						};
						self.cache.store.destroy(&rec);
					}
				}
			}

			// a. The case of new_depth == 0 must be handled specially.
			if new_depth == 0 {
				let key = Key::new(self.id, 0, 0);
				self.cache.destroy_entry(key, &root);
			}
		}
		dbg!();

		// 4. Adjust `TreeData` levels array size, if present.
		if let Some(obj) = self.cache.data.borrow_mut().data.get_mut(&self.id) {
			let mut v = mem::take(&mut obj.data).into_vec();

			// No new records in the higher levels should have been added to the cache.
			// If they are, if means we're either
			// - not holding a resizing lock; or
			// - there is a bug in Cache::flush
			debug_assert!(
				v[new_depth.into()..].iter().all(|m| m.is_empty()),
				"records present in higher levels"
			);

			v.resize_with(new_depth.into(), Default::default);
			obj.data = v.into();
		}
		dbg!();

		// 5. Trim parent records.
		for d in (1..new_depth).rev() {
			dbg!(&d, &self.cache.data.borrow().resizing);
			// Determine max valid offset for current record.
			let max_offset = calc_offset(self.max_record_size(), new_len - 1, d);

			// Collect offsets of entries that need to be removed.
			//
			// We don't operate directly on the cache data because:
			//
			// * We do not want to take entries out and potentially exceed cache budget by a
			//   significant amount
			//   and/or block other tasks from fetching data.
			// * We want to avoid iterating over the hashmap from the start every time, which
			//   is O(n^2)
			//
			// Collecting the offsets beforehand results in O(n) runtime at a relatively small O(n)
			// memory cost.
			let offsets = {
				let data = &mut *self.cache.data.borrow_mut();
				data.data
					.entry(self.id)
					.or_insert_with(|| TreeData::new(new_depth))
					.data[usize::from(d)]
				.keys()
				.copied()
				.filter(|&offt| offt > max_offset)
				.chain(
					data.resizing
						.get(&self.id)
						.unwrap()
						.destroy_records
						.keys()
						.copied()
						.filter(|&(depth, offt)| depth == d && offt > max_offset)
						.map(|(_, offt)| offt),
				)
				.collect::<Vec<_>>()
			};

			// Destroy all subtrees.
			for offset in offsets {
				let key = Key::new(self.id, d, offset);
				let mut record = None;
				for index in 0.. {
					let entry = if let Some(e) = self.cache.get_entry(key) {
						e
					} else {
						// If it's not in the cache, it has been flushed.
						// ditto ditto ditto
						let rec = if let Some(r) = record {
							r
						} else {
							let mut data = self.cache.data.borrow_mut();
							let lock = data.resizing.get_mut(&key.id()).expect("no resizing lock");
							let rec = lock
								.destroy_records
								.remove(&(key.depth(), key.offset()))
								.expect("no entry in resize lock");
							*record.insert(rec)
						};
						self.cache.fetch_entry(key, &rec, cur_depth).await?
					};
					let Some(rec) = get_record(&entry.data, index) else { break };
					drop(entry);
					let offt =
						(offset << rec_size_p2 - RECORD_SIZE_P2) + u64::try_from(index).unwrap();
					let k = Key::new(self.id, d - 1, offt);
					destroy(self.cache, k, &rec, new_depth).await?;
				}
				// Remove the entry if it is still present.
				if self.cache.remove_entry(key).is_none() {
					// If it's not in the cache, it has been flushed.
					// ditto ditto ditto
					let rec = if let Some(r) = record {
						r
					} else {
						let mut data = self.cache.data.borrow_mut();
						let lock = data.resizing.get_mut(&key.id()).expect("no resizing lock");
						let rec = lock
							.destroy_records
							.remove(&(key.depth(), key.offset()))
							.expect("no entry in resize lock");
						*record.insert(rec)
					};
					self.cache.store.destroy(&rec);
				}
			}

			// Trim record at boundary.
			// Destroy rightmost subtrees.
			let max_child_offset = calc_offset(self.max_record_size(), new_len - 1, d - 1);
			let max_index =
				usize::try_from(max_child_offset % (1u64 << self.max_record_size())).unwrap();
			for i in max_index + 1.. {
				let entry = self.get(d, max_offset).await?;
				let Some(rec) = get_record(&entry.data, i) else { break };
				drop(entry);
				let offt = max_child_offset + u64::try_from(i).unwrap();
				let key = Key::new(self.id, d - 1, offt);
				destroy(self.cache, key, &rec, new_depth).await?;
			}
			// Shrink record.
			self.get(d, max_offset)
				.await?
				.modify(|data| {
					let new_rec_len = data.len().min((max_index + 1) * mem::size_of::<Record>());
					data.resize(new_rec_len, 0);
				})
				.await?;
			/*else {
				let (offset, index) = divmod_p2(new_len - 1, rec_size_p2);

				debug_assert_eq!(offset, offset_bound);
				let entry = self.get(d, offset_bound).await?;
				entry
					.modify(|data| {
						let new_rec_len = data.len().min(index + 1);
						data.resize(new_rec_len, 0);
					})
					.await?;
			}*/
		}
		dbg!();

		// 6. Trim rightmost leaf.
		{
			let cut = usize::try_from(new_len % (1u64 << self.max_record_size())).unwrap();
			if cut != 0 {
				self.get(0, (new_len - 1) >> self.max_record_size())
					.await?
					.modify(|data| data.resize(cut, 0))
					.await?;
			}
		}
		dbg!();

		// Presto, at last
		Ok(())
		*/
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

		let cur_len = u64::from(cur_root.total_length);

		debug_assert!(
			cur_len < new_len,
			"new len is equal or smaller than cur len"
		);

		let cur_depth = depth(self.max_record_size(), cur_len);
		let new_depth = depth(self.max_record_size(), new_len);

		let new_root;

		// Check if the depth changed.
		// If so we need to move the current root.
		if cur_depth < new_depth {
			// Resize to account for new depth
			{
				let mut obj = self.cache.get_object_entry_mut(self.id, cur_depth);
				let mut v = mem::take(&mut obj.data).into_vec();
				v.resize_with(new_depth.into(), Default::default);
				obj.data = v.into();
			}

			// Add a new record on top and move the root to it.
			let key = Key::new(self.id, cur_depth, 0);
			let entry = self
				.cache
				.fetch_entry(key, &Record::default(), new_depth)
				.await?;
			entry
				.modify(|data| {
					debug_assert!(data.is_empty(), "data should be empty");
					data.extend_from_slice(
						Record { total_length: 0.into(), references: 0.into(), ..cur_root }
							.as_ref(),
					);
				})
				.await?;

			// New root does not refer to any existing records, so use default.
			new_root = Record {
				total_length: new_len.into(),
				references: cur_root.references,
				..Default::default()
			};
		} else {
			// Just adjust length and presto
			new_root = Record { total_length: new_len.into(), ..cur_root };
		}

		// Fixup root.
		self.cache.set_object_root(self.id, &new_root).await?;

		Ok(())
	}

	/// The length of the record tree in bytes.
	pub async fn len(&self) -> Result<u64, Error<D>> {
		trace!("len id {}", self.id);
		self.root().await.map(|(_, len)| len)
	}

	/// Get the root record and length of this tree.
	// TODO try to avoid boxing (which is what async_recursion does).
	// Can maybe be done in a clean way by abusing generics?
	// i.e. use "marker"/"tag" structs like ObjectTag and ListTag
	#[async_recursion::async_recursion(?Send)]
	async fn root(&self) -> Result<(Record, u64), Error<D>> {
		trace!("root id {}", self.id);
		let root = if self.id == OBJECT_LIST_ID {
			self.cache.object_list()
		} else {
			self.cache.get_object_root(self.id).await?
		};
		Ok((root, root.total_length.into()))
	}

	/// Get a leaf cache entry.
	///
	/// It may fetch up to [`MAX_DEPTH`] of parent entries.
	///
	/// Note that `offset` must already include appropriate shifting.
	// FIXME concurrent resizes will almost certainly screw something internally.
	// Maybe add a per object lock to the cache or something?
	async fn get(&self, target_depth: u8, offset: u64) -> Result<EntryRef<'a, D>, Error<D>> {
		trace!(
			"get id {}, depth {}, offset {}",
			self.id,
			target_depth,
			offset
		);
		// Steps:
		//
		// 2. Find the record or the first ancestor that is present.
		//    If found, extract the proper record from it.
		//    If none are present, take the root record.
		// 1. Check if the entry is present.
		//    If so, just return it.
		// 3. Fetch the data associated with the taken record.
		//    Do this "recursively" until the target is reached.

		let rec_size = self.max_record_size().to_raw();

		let mut cur_depth = target_depth;
		let depth_offset_shift = |d| (rec_size - RECORD_SIZE_P2) * (d - target_depth);

		let (root, len) = self.root().await?;

		// Find the first parent or leaf entry that is present starting from a leaf
		// and work back downwards.

		let obj_depth = depth(self.max_record_size(), len);

		debug_assert!(
			target_depth < obj_depth,
			"target depth exceeds object depth"
		);

		// FIXME we need to be careful with resizes while this task is running.
		// Perhaps lock object IDs somehow?

		// Go up and check if a parent entry is either present or being fetched.
		let entry = future::poll_fn(|cx| {
			while cur_depth < obj_depth {
				let key = Key::new(self.id, cur_depth, offset >> depth_offset_shift(cur_depth));
				// Check if the entry is already present.
				let (data, lrus) = RefMut::map_split(self.cache.data.borrow_mut(), |data| {
					(&mut data.data, &mut data.lrus)
				});
				if let Ok(entry) = RefMut::filter_map(data, |data| key.get_entry_mut(data)) {
					return Poll::Ready(Some(EntryRef::new(self.cache, key, entry, lrus)));
				}
				drop(lrus);
				let mut data = self.cache.data.borrow_mut();
				// Check if another task is already fetching the entry we need.
				if let Some(wakers) = data.fetching.get_mut(&key) {
					wakers.push(cx.waker().clone());
					return Poll::Pending;
				}
				cur_depth += 1;
			}
			Poll::Ready(None)
		})
		.await;

		// Get first record to fetch.
		let mut record;
		// Check if we found any cached record at all.
		if let Some(entry) = entry {
			if cur_depth == target_depth {
				// The entry we need is already present
				return Ok(entry);
			}

			// Start from a parent record.
			debug_assert!(cur_depth < obj_depth, "parent should be below root");
			cur_depth -= 1;
			let offt = offset >> depth_offset_shift(cur_depth);
			let index = (offt % (1 << rec_size - RECORD_SIZE_P2))
				.try_into()
				.unwrap();
			record = get_record(&entry.data, index).unwrap_or_default();
		} else {
			// Start from the root.
			debug_assert_eq!(cur_depth, obj_depth, "root should be at obj_depth");
			record = root;
			cur_depth -= 1;
		}

		// Fetch records until we can lock the one we need.
		debug_assert!(cur_depth >= target_depth);
		let entry = loop {
			if record.length == 0 {
				// Skip straight to the end since it's all zeroes from here on anyways.
				let key = Key::new(self.id, target_depth, offset);
				return self
					.cache
					.fetch_entry(key, &Record::default(), obj_depth)
					.await;
			}

			let key = Key::new(self.id, cur_depth, offset >> depth_offset_shift(cur_depth));
			let entry = self.cache.fetch_entry(key, &record, obj_depth).await?;

			// Check if we got the record we need.
			if cur_depth == target_depth {
				break entry;
			}

			cur_depth -= 1;

			// Fetch the next record.
			let offt = offset >> depth_offset_shift(cur_depth);
			let index = (offt % (1 << rec_size - RECORD_SIZE_P2))
				.try_into()
				.unwrap();
			record = get_record(&entry.data, index).unwrap_or_default();
		};

		Ok(entry)
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
	pub async fn replace_with(&self, other: Tree<'a, D>) -> Result<(), Error<D>> {
		// FIXME check locks
		self.cache.move_object(other.id, self.id).await
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

		// Use get_object_root as the object list doesn't use reference counting.
		let mut root = self.cache.get_object_root(self.id).await?;
		debug_assert!(root.references != 0, "invalid object");
		if root.references == u16::MAX {
			return Ok(false);
		}
		root.references += 1;
		self.cache.set_object_root(self.id, &root).await?;

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

		// Use get_object_root as the object list doesn't use reference counting.
		let mut root = self.cache.get_object_root(self.id).await?;
		debug_assert!(root.references != 0, "invalid object");
		root.references -= 1;
		self.cache.set_object_root(self.id, &root).await?;

		if root.references == 0 {
			// Free space.
			self.resize(0).await?;
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
}

/// Determine offset of a record given a depth and byte offset.
pub(super) fn calc_offset(record_size: MaxRecordSize, byte_offset: u64, depth: u8) -> u64 {
	let mut offt = byte_offset >> record_size;
	for _ in 0..depth {
		offt >>= record_size.to_raw() - RECORD_SIZE_P2;
	}
	offt
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
