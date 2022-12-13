use {
	super::{Cache, CacheRef, FlushLock, TreeData, OBJECT_LIST_ID},
	crate::{util::get_record, Dev, Error, MaxRecordSize, Record},
	core::{future::Future, mem, ops::RangeInclusive, pin::Pin},
	std::collections::hash_map,
};

const RECORD_SIZE: u64 = mem::size_of::<Record>() as _;
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

		// Lock object now so it doesn't get evicted.
		let mut data = cache.data.borrow_mut();
		*data.locked_objects.entry(id).or_default() += 1;

		if !{ data }.data.contains_key(&id) {
			// Get length
			let mut rec = Record::default();
			cache.read_object_table(id, rec.as_mut()).await?;
			let length = rec.total_length.into();
			cache
				.data
				.borrow_mut()
				.data
				.insert(id, TreeData::new(depth(cache.max_record_size(), length)));
		}

		Ok(Self { cache, id })
	}

	/// Access the object list.
	pub(super) async fn new_object_list(cache: &'a Cache<D>) -> Result<Tree<'a, D>, Error<D>> {
		let id = OBJECT_LIST_ID;

		// Lock object now so it doesn't get evicted.
		let mut data = cache.data.borrow_mut();
		*data.locked_objects.entry(id).or_default() += 1;

		match data.data.entry(id) {
			hash_map::Entry::Occupied(_) => {}
			hash_map::Entry::Vacant(e) => {
				let length = u64::from(cache.store.object_list().total_length);
				e.insert(TreeData::new(depth(cache.max_record_size(), length)));
			}
		}

		drop(data);
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
		let len = self.len().await?;

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
			let b = self.get(0, *range.start()).await?;

			let mut b = b.get_mut().await?;

			let b = &mut b.data;

			let min_len = last_offset.max(b.len());
			b.resize(min_len, 0);
			b[first_offset..last_offset].copy_from_slice(data);
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

				let b = self.get(0, first_key).await?;
				let mut b = b.get_mut().await?;
				let b = &mut b.data;

				b.resize(first_offset, 0);
				b.extend_from_slice(d);
			}

			// Copy middle records |xxxxxxxx|
			for key in range {
				let d;
				(d, data) = data.split_at(1usize << self.max_record_size());

				// "Fetch" directly since we're overwriting the entire record anyways.
				let b = self
					.cache
					.fetch_entry(self.id, 0, key, &Record::default())
					.await?;

				let mut b = b.get_mut().await?;
				let b = &mut b.data;

				// If the record was already fetched, it'll have ignored the &Record::default().
				// Hence we need to clear it manually.
				b.clear();
				b.extend_from_slice(d);
			}

			// Copy end record |xxxx----|
			// Don't bother if there is no data
			if last_offset > 0 {
				debug_assert_eq!(data.len(), last_offset);
				let b = self.get(0, last_key).await?;
				let mut b = b.get_mut().await?;
				let b = &mut b.data;

				let min_len = b.len().max(data.len());
				b.resize(min_len, 0);
				b[..last_offset].copy_from_slice(data);
			}
		}

		Ok(data.len())
	}

	/// Read data from a range.
	///
	/// Returns the actual amount of bytes read.
	/// It may exit early if not all data is cached.
	pub async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
		trace!("read id {}, offset {}, len {}", self.id, offset, buf.len());
		let len = self.len().await?;

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

			let b = b.get();

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
				let d = &d.get().data;
				copy(b, d.get(first_offset..).unwrap_or(&[]));
			}

			// Copy middle records |xxxxxxxx|
			for key in range {
				let b;
				(b, buf) = buf.split_at_mut(1usize << self.max_record_size());
				let d = self.get(0, key).await?;
				copy(b, &d.get().data);
			}

			// Copy end record |xxxx----|
			// Don't bother if there's nothing to copy
			if last_offset > 0 {
				debug_assert_eq!(buf.len(), last_offset);
				let d = self.get(0, last_key).await?;
				let d = &d.get().data;
				let max_len = d.len().min(buf.len());
				copy(buf, &d[..max_len]);
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
				"update_record replace root ({}, {}) -> ({}, {})",
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
			let mut entry = entry.get_mut().await?;

			// Destroy old record
			let old_record = get_record(&entry.data, index);
			trace!(
				"update_record replace parent ({}, {}) -> ({}, {})",
				record.lba,
				record.length,
				old_record.lba,
				old_record.length
			);
			self.cache.store.destroy(&old_record);

			// Calc offset in parent
			let index = index * mem::size_of::<Record>();
			let min_len = entry.data.len().max(index + mem::size_of::<Record>());

			// Store new record
			entry.data.resize(min_len, 0);
			entry.data[index..index + mem::size_of::<Record>()].copy_from_slice(record.as_ref());

			let old_record2 = get_record(&entry.data, index);
			(old_record.length > 0 && old_record2.length > 0)
				.then(|| assert_ne!(old_record.lba, old_record2.lba));
			Ok(())
		}
	}

	/// Resize record tree.
	pub async fn resize(&self, new_len: u64) -> Result<(), Error<D>> {
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
		// Shrinking a tree is tricky, especially in combination with unflushed growths.
		//
		// The steps to take are as follows:
		//
		// * Readjust the root.
		//   This is necessary so records can safely be removed.
		//   Flushes may happen while a resize is in progress which may interfere.
		//   It also allows reads & writes to continue during the resize.
		//   This adjusts the effective depth though the TreeData's depth array is not shrunk yet.
		//
		// * Destroy out-of-range records.
		//   There are two parts to this:
		//   * Destroy on-dev records
		//     This starts from the dev root and goes downwards.
		//     When this process is done the depth array of the respective TreeData can be shrunk.
		//     Note that any dirty parent records are properly accounted for as all fetches go
		//     through the cache.
		//     Hence no use-after-frees or double-frees will occur.
		//   * Destroy cache-only records.
		//     When growing a new root is not immediately made.
		//     Instead, dirty records bubble up and eventually hit a new root.
		//     These dirty records may not be written to disk yet, so scan the cache separately
		//     for those records and apply the same recursive destroy function.
		//
		// * Trim records.
		//   To ensure that new bytes from a grow later on are all zeroes it is necessary to
		//   zero out bytes in records that extends past the length.
		//   To keep things fast for large objects (in terms of total length),
		//   this process occurs top-down.
		//   For parent nodes, if a child is out of range,
		//   remove the record and destroy the subtree.
		//   For the leaf node, just resize.

		let rec_size_p2 = self.max_record_size().to_raw();

		// Prevent flushing as we'll be operating directly on the cache data of this tree.
		let _flush_lock = FlushLock::new(&self.cache.data, self.id).await;

		let cur_len = u64::from(cur_root.total_length);

		let cur_depth = depth(self.max_record_size(), cur_len);
		let new_depth = depth(self.max_record_size(), new_len);

		debug_assert!(cur_len > new_len, "new_len is equal or larger than cur len");

		// Special-case 0 so we can avoid some annoying & hard-to-read checks below.
		//
		// Especially, avoid interpreting data in leaf records as records.
		//
		// It should also makes destroying objects a bit faster, which is nice.
		if new_len == 0 {
			// Clear root
			let new_root = Record {
				total_length: new_len.into(),
				references: cur_root.references,
				..Default::default()
			};
			self.cache.set_object_root(self.id, &new_root).await?;

			// Destroy all records.
			if cur_depth > 0 {
				destroy(self, cur_depth - 1, 0, &cur_root).await?;
			}

			// Destroy parents
			for d in (1..cur_depth).rev() {
				let entries =
					mem::take(&mut self.cache.get_object_entry_mut(self.id).data[usize::from(d)]);
				for (offt, entry) in entries {
					for index in 0..1 << rec_size_p2 - RECORD_SIZE_P2 {
						let rec = get_record(&entry.data, index.try_into().unwrap());
						destroy(
							self,
							d - 1,
							(offt << rec_size_p2 - RECORD_SIZE_P2) + index,
							&rec,
						)
						.await?;
					}
					self.cache
						.data
						.borrow_mut()
						.lrus
						.adjust_cache_removed_entry(&entry);
				}
			}

			// Destroy leaves
			{
				let mut data = self.cache.data.borrow_mut();
				let entries = mem::take(
					&mut data
						.data
						.get_mut(&self.id)
						.expect("cache entry by id not present")
						.data[0],
				);
				for (_, entry) in entries {
					data.lrus.adjust_cache_removed_entry(&entry);
				}
			}

			// Clear object depth array.
			self.cache.get_object_entry_mut(self.id).data = [].into();

			return Ok(());
		}

		// Readjust the root.
		//
		// Only necessary if the depth changes.

		// Get & set new root
		{
			let new_root = if new_depth < cur_depth {
				// Make child record the new root.
				let entry = self.get(new_depth, 0).await?;
				let rec = get_record(&entry.get().data, 0);
				Record { total_length: new_len.into(), references: cur_root.references, ..rec }
			} else {
				// Only adjust length.
				Record { total_length: new_len.into(), ..cur_root }
			};
			self.cache.set_object_root(self.id, &new_root).await?;
		}

		// Destroy out-of-range records
		//
		// Do note that we *cannot* use Tree::get, we have to use Cache::fetch_entry directly.

		/// Destroy all records in a subtree recursively.
		async fn destroy<'a, D: Dev>(
			tree: &Tree<'a, D>,
			depth: u8,
			offset: u64,
			record: &Record,
		) -> Result<(), Error<D>> {
			trace!(
				"shrink::destroy id {}, depth {}, offset {}, record.(lba, length) ({}, {})",
				tree.id,
				depth,
				offset,
				record.lba,
				record.length,
			);
			// The actually recursive part, which does require boxing the future
			#[async_recursion::async_recursion(?Send)]
			async fn f<'a, D: Dev>(
				tree: &Tree<'a, D>,
				depth: u8,
				offset: u64,
				record: &Record,
			) -> Result<(), Error<D>> {
				// We're a parent node, destroy children.
				let entry = tree
					.cache
					.fetch_entry(tree.id, depth, offset, record)
					.await?;
				let records_per_parent_p2 = tree.max_record_size().to_raw() - RECORD_SIZE_P2;
				for index in 0..1usize << records_per_parent_p2 {
					let rec = get_record(&entry.get().data, index);
					let offt = (offset << records_per_parent_p2) + u64::try_from(index).unwrap();
					destroy(tree, depth - 1, offt, &rec).await?;
				}
				Ok(())
			}

			if depth > 0 && record.length > 0 {
				f(tree, depth, offset, record).await?
			}

			// Entry may not be present, so don't use tree.cache.destroy
			let _ = tree.cache.remove_entry(tree.id, depth, offset);
			tree.cache.store.destroy(record);

			Ok(())
		}

		// Destroy records.
		// Start with root, then cache-only records.
		if new_depth < cur_depth {
			let entry = self
				.cache
				.fetch_entry(self.id, cur_depth - 1, 0, &cur_root)
				.await?;
			let data = entry.destroy(&cur_root);
			// Destroy every subtree except the leftmost one, as we still need that one.
			for index in 1..1 << rec_size_p2 - RECORD_SIZE_P2 {
				let rec = get_record(&data, index);
				destroy(self, cur_depth - 2, index.try_into().unwrap(), &rec).await?;
			}
		}

		// Destroy cache-only records.
		for d in (new_depth..cur_depth).rev() {
			// None of the entries remaining in this level need to be kept, so just take them out.
			let mut data = self.cache.data.borrow_mut();
			let obj = data.data.get_mut(&self.id).expect("no cache entry with id");
			let entries = mem::take(&mut obj.data[usize::from(d)]);

			// The entries haven't been flushed nor do they have any on-dev allocations, so just
			// remove them from LRUs.
			for entry in entries.values() {
				data.lrus.adjust_cache_removed_entry(&entry);
			}

			// Destroy all subtrees.
			drop(data);
			for (offset, entry) in entries {
				let skip_first = (offset == 0).into();
				for index in skip_first..1 << rec_size_p2 - RECORD_SIZE_P2 {
					let rec = get_record(&entry.data, index.try_into().unwrap());
					let offt = (offset << rec_size_p2 - RECORD_SIZE_P2) + index;
					destroy(self, d - 1, offt, &rec).await?;
				}
			}
		}

		// Adjust amount of levels in object cache.
		{
			let mut obj = self.cache.get_object_entry_mut(self.id);
			let mut v = mem::take(&mut obj.data).into_vec();

			// FIXME during destroy() records may have been flushed, recreating destroyed parents.
			// At least, in theory. Add a debug_assert just in case
			debug_assert!(
				v[new_depth.into()..].iter().all(|m| m.is_empty()),
				"recreated parent"
			);

			v.resize_with(new_depth.into(), Default::default);
			obj.data = v.into();
		}

		// We need to be careful here not to destroy too much.
		for d in (0..new_depth).rev() {
			// In case of overflow, assume 0
			// Should only happen for root records, where we want 0 anyways.
			let offset_bound = (new_len - 1)
				.checked_shr((rec_size_p2 + (rec_size_p2 - RECORD_SIZE_P2) * d).into())
				.unwrap_or(0);

			// None of the entries remaining in this level need to be kept, so just take them out.
			let mut data = self.cache.data.borrow_mut();
			let obj = data.data.get_mut(&self.id).expect("no cache entry with id");
			let entries = obj.data[usize::from(d)]
				.drain_filter(|&offt, _| offt > offset_bound)
				.collect::<Vec<_>>();

			// The entries haven't been flushed nor do they have any on-dev allocations, so just
			// remove them from LRUs.
			for (_, entry) in entries.iter() {
				data.lrus.adjust_cache_removed_entry(&entry);
			}
			drop(data);

			// Destroy all subtrees.
			if d > 0 {
				for (offset, entry) in entries {
					for index in 0..1 << rec_size_p2 - RECORD_SIZE_P2 {
						let rec = get_record(&entry.data, index);
						let offt = (offset << rec_size_p2 - RECORD_SIZE_P2)
							+ u64::try_from(index).unwrap();
						destroy(self, d - 1, offt, &rec).await?;
					}
				}
			}

			// Trim record at boundary
			if d > 0 {
				let offt = (new_len - 1) >> rec_size_p2 + (rec_size_p2 - RECORD_SIZE_P2) * (d - 1);
				let (offset, index) = divmod_p2(offt, rec_size_p2 - RECORD_SIZE_P2);

				debug_assert_eq!(offset, offset_bound);
				let entry = self.get(d, offset).await?;
				// Destroy out-of-range subtrees.
				for i in index + 1..1 << rec_size_p2 - RECORD_SIZE_P2 {
					let rec = get_record(&entry.get().data, i);
					let offt = (offset << rec_size_p2 - RECORD_SIZE_P2) + u64::try_from(i).unwrap();
					destroy(self, d - 1, offt, &rec).await?;
				}
				let mut entry = entry.get_mut().await?;
				let new_rec_len = entry.data.len().min((index + 1) * mem::size_of::<Record>());
				entry.data.resize(new_rec_len, 0);
			} else {
				let (offset, index) = divmod_p2(new_len - 1, rec_size_p2);

				debug_assert_eq!(offset, offset_bound);
				let entry = self.get(d, offset_bound).await?;
				let mut entry = entry.get_mut().await?;
				let new_rec_len = entry.data.len().min(index + 1);
				entry.data.resize(new_rec_len, 0);
			}
		}

		// Presto, at last
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

		let _flush_lock = FlushLock::new(&self.cache.data, self.id).await;

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
				let mut obj = self.cache.get_object_entry_mut(self.id);
				let mut v = mem::take(&mut obj.data).into_vec();
				v.resize_with(new_depth.into(), Default::default);
				obj.data = v.into();
			}

			// Add a new record on top and move the root to it.
			{
				let entry = self
					.cache
					.fetch_entry(self.id, cur_depth, 0, &Record::default())
					.await?;
				let mut entry = entry.get_mut().await?;
				debug_assert!(entry.data.is_empty(), "data should be empty");
				entry.data.extend_from_slice(
					Record { total_length: 0.into(), references: 0.into(), ..cur_root }.as_ref(),
				);
			}

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
		self.root().await.map(|(_, len)| len)
	}

	/// Get the root record and length of this tree.
	// TODO try to avoid boxing (which is what async_recursion does).
	// Can maybe be done in a clean way by abusing generics?
	// i.e. use "marker"/"tag" structs like ObjectTag and ListTag
	#[async_recursion::async_recursion(?Send)]
	async fn root(&self) -> Result<(Record, u64), Error<D>> {
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
	async fn get(&self, target_depth: u8, offset: u64) -> Result<CacheRef<D>, Error<D>> {
		trace!(
			"get id {}, depth {}, offset {}",
			self.id,
			target_depth,
			offset
		);
		// This is very intentionally not recursive,
		// as you can't have async recursion without boxing.

		let rec_size = self.max_record_size().to_raw();

		let mut cur_depth = target_depth;
		let depth_offset_shift = |d| (rec_size - RECORD_SIZE_P2) * (d - target_depth);

		let (root, len) = self.root().await?;

		// Find the first parent or leaf entry that is present starting from a leaf
		// and work back downwards.

		let cache_depth = depth(self.max_record_size(), len);
		let dev_depth = depth(self.max_record_size(), root.total_length.into());

		debug_assert!(
			target_depth < cache_depth,
			"target depth exceeds object depth"
		);

		// FIXME we need to be careful with resizes while this task is running.
		// Perhaps lock object IDs somehow?

		// Go up
		// TODO has_entry doesn't check if an entry is already being fetched.
		while cur_depth < cache_depth
			&& !self
				.cache
				.has_entry(self.id, cur_depth, offset >> depth_offset_shift(cur_depth))
		{
			cur_depth += 1;
		}

		if cur_depth == target_depth {
			// The entry we need is already present
			// *or* the entry is newly created from a grow and zeroed.
			// TODO verify the latter statement.
			return self
				.cache
				.fetch_entry(self.id, cur_depth, offset, &Record::default())
				.await;
		}

		// Get first record to fetch.
		let mut record;
		// Check if we found any cached record at all.
		if cur_depth == cache_depth {
			// Check if the record we're trying to fetch is within the newly added region from
			// growing the tree or within the old region.
			//
			// If is in the new region, add a zero record and return immediately.
			//
			// If not, update cur_depth to match the old depth and fetch as normal.

			let shift = rec_size + (rec_size - RECORD_SIZE_P2) * target_depth;
			// The shift may be 64 or larger if we're close to the root.
			// With large offsets this may overflow, so use u128.
			let offset_byte = u128::from(offset) << shift;

			if offset_byte >= u128::from(u64::from(root.total_length)) || target_depth >= dev_depth
			{
				// Just insert a zeroed record and return that.
				return self
					.cache
					.fetch_entry(self.id, target_depth, offset, &Record::default())
					.await;
			}

			// Start iterating on on-dev records.
			record = root;
			cur_depth = dev_depth;

			cur_depth -= 1;
		} else {
			let offt = offset >> depth_offset_shift(cur_depth);
			let data = self.cache.get_entry(self.id, cur_depth, offt);

			cur_depth -= 1;

			let offt = offset >> depth_offset_shift(cur_depth);
			let index = (offt % (1 << rec_size - RECORD_SIZE_P2))
				.try_into()
				.unwrap();
			record = get_record(&data.data, index);
		}

		// Fetch records until we can lock the one we need.
		debug_assert!(cur_depth >= target_depth);
		let entry = loop {
			let offt = offset >> depth_offset_shift(cur_depth);
			let entry = self
				.cache
				.fetch_entry(self.id, cur_depth, offt, &record)
				.await?;

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
			record = get_record(&entry.get().data, index);
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

	/// Get the maximum record size.
	fn max_record_size(&self) -> MaxRecordSize {
		self.cache.max_record_size()
	}

	/// Get the ID of this object.
	pub fn id(&self) -> u64 {
		self.id
	}
}

impl<D: Dev> Drop for Tree<'_, D> {
	fn drop(&mut self) {
		let data = { &mut *self.cache.data.borrow_mut() };
		let hash_map::Entry::Occupied(mut o) = data.locked_objects.entry(self.id) else {
			panic!("object not present")
		};
		*o.get_mut() -= 1;
		if *o.get() == 0 {
			o.remove_entry();
		}
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
fn depth(max_record_size: MaxRecordSize, len: u64) -> u8 {
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
