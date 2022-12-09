use {
	super::{Cache, CacheRef, TreeData, OBJECT_LIST_ID},
	crate::{
		util::{get_record, trim_zeros_end},
		Dev, Error, MaxRecordSize, Record,
	},
	core::{mem, ops::RangeInclusive},
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
			cache.data.borrow_mut().data.insert(
				id,
				TreeData {
					length,
					data: (0..depth(cache.max_record_size(), length))
						.map(|_| Default::default())
						.collect(),
				},
			);
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
				e.insert(TreeData {
					length,
					data: (0..depth(cache.max_record_size(), length))
						.map(|_| Default::default())
						.collect(),
				});
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
			let (old_len, new_len);
			{
				// We need to slice one record twice
				let b = self.get(0, *range.start()).await?;

				let mut b = b.get_mut().await?;

				let b = &mut b.data;
				old_len = b.len();

				let min_len = last_offset.max(b.len());
				b.resize(min_len, 0);
				b[first_offset..last_offset].copy_from_slice(data);
				trim_zeros_end(b);

				new_len = b.len();
			}
			self.cache.adjust_cache_use_both(old_len, new_len).await?;
		} else {
			// We need to slice the first & last record once and operate on the others in full.
			let mut data = data;
			let mut range = range.into_iter();

			let first_key = range.next().unwrap();
			let last_key = range.next_back().unwrap();

			// Copy to first record |----xxxx|
			let (old_len, new_len);
			{
				let d;
				(d, data) = data.split_at((1usize << self.max_record_size()) - first_offset);

				let b = self.get(0, first_key).await?;
				let mut b = b.get_mut().await?;
				let b = &mut b.data;
				old_len = b.len();

				b.resize(first_offset, 0);
				b.extend_from_slice(d);
				trim_zeros_end(b);
				new_len = b.len();
			}
			self.cache.adjust_cache_use_both(old_len, new_len).await?;

			// Copy middle records |xxxxxxxx|
			for key in range {
				let (old_len, new_len);
				{
					let d;
					(d, data) = data.split_at(1usize << self.max_record_size());

					// "Fetch" directly since we're overwriting the entire record anyways.
					let b = self
						.cache
						.fetch_entry(self.id, 0, key, &Record::default())
						.await?;

					let mut b = b.get_mut().await?;
					let b = &mut b.data;
					old_len = b.len();

					// If the record was already fetched, it'll have ignored the &Record::default().
					// Hence we need to clear it manually.
					b.clear();
					b.extend_from_slice(d);
					trim_zeros_end(b);
					new_len = b.len();
				}
				self.cache.adjust_cache_use_both(old_len, new_len).await?;
			}

			// Copy end record |xxxx----|
			// Don't bother if there is no data
			if last_offset > 0 {
				let (old_len, new_len);
				{
					debug_assert_eq!(data.len(), last_offset);
					let b = self.get(0, last_key).await?;
					let mut b = b.get_mut().await?;
					let b = &mut b.data;
					old_len = b.len();

					let min_len = b.len().max(data.len());
					b.resize(min_len, 0);
					b[..last_offset].copy_from_slice(data);
					trim_zeros_end(b);
					new_len = b.len();
				}
				self.cache.adjust_cache_use_both(old_len, new_len).await?;
			}
		}

		Ok(data.len())
	}

	/// Read data from a range.
	///
	/// Returns the actual amount of bytes read.
	/// It may exit early if not all data is cached.
	pub async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
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
		let len = self.len().await?;
		let cur_depth = depth(self.max_record_size(), len);
		let parent_depth = record_depth + 1;
		assert!(parent_depth <= cur_depth);
		if cur_depth == parent_depth {
			assert_eq!(offset, 0, "root can only be at offset 0");

			// Copy total length and references to new root.
			let root = self.root().await?;
			let record = Record { total_length: len.into(), references: root.references, ..record };

			// Update the root.
			if self.id == OBJECT_LIST_ID {
				// Object list root is in header.
				self.cache.store.destroy(&self.cache.store.object_list());
				self.cache.store.set_object_list(record);
				Ok(())
			} else {
				// Object root is in object list.
				// Destroy old root
				let mut old_rec = Record::default();
				let l = self
					.cache
					.read_object_table(self.id, old_rec.as_mut())
					.await?;
				assert_eq!(l, 32, "old root was not fully read");
				self.cache.store.destroy(&old_rec);

				// Store new root
				let l = self
					.cache
					.write_object_table(self.id, record.as_ref())
					.await?;
				assert_eq!(l, 32, "new root was not fully written");
				Ok(())
			}
		} else {
			// Update a parent record.
			let (old_len, new_len);
			{
				// Find parent
				let shift = self.max_record_size().to_raw() - RECORD_SIZE_P2;
				let (offt, index) = divmod_p2(offset, shift);

				let entry = self.get(parent_depth, offt).await?;
				let mut entry = entry.get_mut().await?;

				// Destroy old record
				self.cache.store.destroy(&get_record(&entry.data, index));

				// Calc offset in parent
				old_len = entry.data.len();
				let index = index * mem::size_of::<Record>();
				let min_len = old_len.max(index + mem::size_of::<Record>());

				// Store new record
				entry.data.resize(min_len, 0);
				entry.data[index..index + mem::size_of::<Record>()]
					.copy_from_slice(record.as_ref());
				trim_zeros_end(&mut entry.data);
				new_len = entry.data.len();
			}
			self.cache.adjust_cache_use_both(old_len, new_len).await
		}
	}

	/// Resize record tree.
	pub async fn resize(&self, new_len: u64) -> Result<(), Error<D>> {
		let len = self.len().await?;
		if new_len < len {
			self.shrink(new_len).await
		} else if new_len > len {
			self.grow(new_len).await
		} else {
			Ok(())
		}
	}

	/// Shrink record tree.
	async fn shrink(&self, new_len: u64) -> Result<(), Error<D>> {
		debug_assert!(
			self.len().await? > new_len,
			"new_len is equal or larger than cur len"
		);

		/// Destroy all records in a subtree recursively.
		async fn destroy<'a, D: Dev>(
			tree: &Tree<'a, D>,
			depth: u8,
			offset: u64,
			record: &Record,
		) -> Result<(), Error<D>> {
			// The actually recursive part, which does require boxing the future
			#[async_recursion::async_recursion(?Send)]
			async fn f<'a, D: Dev>(
				tree: &Tree<'a, D>,
				depth: u8,
				offset: u64,
			) -> Result<(), Error<D>> {
				// We're a parent node, destroy children.
				let entry = tree.get(depth, offset).await?;
				for offt in 1..1usize << tree.max_record_size() {
					let rec = get_record(&entry.get().data, offt);
					let offt = (offset << tree.max_record_size()) + u64::try_from(offt).unwrap();
					destroy(tree, depth - 1, offt, &rec).await?;
				}
				Ok(())
			}
			if depth > 0 && record.length > 0 {
				f(tree, depth, offset).await?
			}
			tree.cache.destroy(tree.id, depth, offset, record);
			Ok(())
		}

		// Reduce depth & remove completely out of range entries.
		let cur_depth = depth(self.max_record_size(), self.len().await?);
		let new_depth = depth(self.max_record_size(), new_len);

		// Deallocate records that are out of range.
		for d in (new_depth..cur_depth).rev() {
			// Get current root node.
			let entry = self.get(d, 0).await?;
			// Destroy every subtree after the first child.
			// Do check if d > 0, otherwise it's a leaf and hence has no children.
			if d > 0 {
				for offt in 1..1 << self.max_record_size().to_raw() {
					let rec = get_record(&entry.get().data, offt);
					destroy(self, d - 1, u64::try_from(offt).unwrap(), &rec).await?;
				}
			}
		}

		// Trim last record on the right, if necessary.
		let (mut offt, i) = divmod_p2(new_len, self.max_record_size().to_raw());
		if i > 0 {
			let entry = self.get(0, offt).await?;
			let mut e = entry.get_mut().await?;
			if i < e.data.len() {
				let old_len = e.data.len();
				e.data.resize(i, 0);
				trim_zeros_end(&mut e.data);
				let new_len = e.data.len();
				drop(e);
				drop(entry);
				self.cache.adjust_cache_use_both(old_len, new_len).await?;
			}
			offt += 1;
		}

		let data = { &mut *self.cache.data.borrow_mut() };
		let obj = { data.data.get_mut(&self.id).expect("no entry for object") };

		// Remove cache entries that are out of range.
		for level in obj.data.iter_mut() {
			for (_, entry) in level.drain_filter(|o, _| *o >= offt) {
				// Remove from LRUs
				data.global_lru.remove(entry.global_index);
				data.global_cache_size -= entry.data.len();
				if let Some(idx) = entry.write_index {
					data.write_lru.remove(idx);
					data.write_cache_size -= entry.data.len();
				}
			}
			offt >>= self.max_record_size().to_raw() - RECORD_SIZE_P2;
		}

		// Reduce depth.
		let mut v = mem::take(&mut obj.data).into_vec();
		v.resize_with(new_depth.into(), Default::default);
		obj.data = v.into();

		// Set root
		debug_assert!(
			obj.data.last().map_or(true, |m| m.len() <= 1),
			"root level has multiple entries"
		);
		debug_assert!(
			obj.data
				.last()
				.and_then(|m| m.keys().next())
				.map_or(true, |o| *o == 0),
			"root level entry has offset greater than 0"
		);

		// Set length
		obj.length = new_len;

		Ok(())
	}

	/// Grow record tree.
	async fn grow(&self, new_obj_len: u64) -> Result<(), Error<D>> {
		debug_assert!(
			self.len().await? < new_obj_len,
			"new_obj_len is equal or smaller than cur len"
		);

		// Increase depth.
		let cur_depth = depth(self.max_record_size(), self.len().await?);
		let new_depth = depth(self.max_record_size(), new_obj_len);

		let root = self.root().await?;

		let (old_len, new_len);
		if cur_depth < new_depth {
			// Resize to account for new depth
			{
				let mut data = self.cache.data.borrow_mut();
				let data = data.data.get_mut(&self.id).expect("no entry for object");
				let mut v = mem::take(&mut data.data).into_vec();
				v.resize_with(new_depth.into(), Default::default);
				data.data = v.into();
			}

			// Add a single new, empty record and add the current root as a child to it.
			// The changes will propagate up.
			let entry = self
				.cache
				.fetch_entry(self.id, cur_depth, 0, &Record::default())
				.await?;
			let mut entry = entry.get_mut().await?;

			// Zero out unused fields
			old_len = entry.data.len();
			entry.data.extend_from_slice(
				Record { total_length: 0.into(), references: 0.into(), ..root }.as_ref(),
			);
			trim_zeros_end(&mut entry.data);
			new_len = entry.data.len();
			drop(entry);
		} else {
			(old_len, new_len) = (0, 0);
		}

		self.cache
			.data
			.borrow_mut()
			.data
			.get_mut(&self.id)
			.expect("object is not cached")
			.length = new_obj_len;

		// Adjusting cache size may lead to a flush, so make sure the object length
		// is corrected before calling it.
		self.cache.adjust_cache_use_both(old_len, new_len).await?;

		Ok(())
	}

	/// The length of the record tree in bytes.
	pub async fn len(&self) -> Result<u64, Error<D>> {
		Ok(self
			.cache
			.data
			.borrow()
			.data
			.get(&self.id)
			.expect("object not in cache")
			.length)
	}

	/// Get the root record of this tree.
	// TODO try to avoid boxing (which is what async_recursion does).
	// Can maybe be done in a clean way by abusing generics?
	// i.e. use "marker"/"tag" structs like ObjectTag and ListTag
	#[async_recursion::async_recursion(?Send)]
	pub async fn root(&self) -> Result<Record, Error<D>> {
		if self.id == OBJECT_LIST_ID {
			Ok(self.cache.object_list())
		} else {
			let list = Self::new(self.cache, OBJECT_LIST_ID).await?;
			let mut record = Record::default();
			list.read(self.id * RECORD_SIZE, record.as_mut()).await?;
			Ok(record)
		}
	}

	/// Get a leaf cache entry.
	///
	/// It may fetch up to [`MAX_DEPTH`] of parent entries.
	///
	/// Note that `offset` must already include appropriate shifting.
	// FIXME concurrent resizes will almost certainly screw something internally.
	// Maybe add a per object lock to the cache or something?
	async fn get(&self, target_depth: u8, offset: u64) -> Result<CacheRef<D>, Error<D>> {
		// This is very intentionally not recursive,
		// as you can't have async recursion without boxing.

		let rec_size = self.max_record_size().to_raw();

		let mut cur_depth = target_depth;
		let depth_offset_shift = |d| (rec_size - RECORD_SIZE_P2) * (d - target_depth);

		let root = self.root().await?;
		let len = self.len().await?;

		// Find the first parent or leaf entry that is present starting from a leaf
		// and work back downwards.

		let cache_depth = depth(self.max_record_size(), len);
		let dev_depth = depth(self.max_record_size(), root.total_length.into());

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

			if offset_byte >= u128::from(u64::from(root.total_length)) || target_depth > dev_depth {
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
		*o.get_mut() += 1;
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
fn depth(max_record_size: MaxRecordSize, mut len: u64) -> u8 {
	if len == 0 {
		0
	} else {
		let mut depth = 0;
		while len > 1u64 << max_record_size {
			len >>= max_record_size.to_raw() - RECORD_SIZE_P2;
			depth += 1;
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
	fn depth_rec1k_2p40() {
		assert_eq!(depth(MaxRecordSize::K1, 1 << 10 + 5 * 6), 7);
	}

	#[test]
	fn depth_rec1k_lenmax() {
		assert_eq!(depth(MaxRecordSize::K1, u64::MAX), 12);
	}
}
