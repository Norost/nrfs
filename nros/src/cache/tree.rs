use {
	super::{Cache, CacheData, CacheRef, OBJECT_LIST_ID},
	crate::{Dev, DevSet, Error, MaxRecordSize, Record, Store},
	core::{
		cell::{RefCell, RefMut},
		mem,
		ops::RangeInclusive,
	},
	std::{
		collections::btree_map::{BTreeMap, Entry},
		rc::Rc,
	},
};

const RECORD_SIZE: u64 = mem::size_of::<Record>() as _;
const RECORD_SIZE_P2: u8 = mem::size_of::<Record>().ilog2() as _;

/// Implementation of a record tree.
#[derive(Clone, Debug)]
pub struct Tree<D: Dev> {
	/// Underlying cache.
	cache: Rc<Cache<D>>,
	/// ID of the object.
	id: u64,
}

impl<D: Dev> Tree<D> {
	pub(super) fn new(cache: Rc<Cache<D>>, id: u64) -> Self {
		Self { cache, id }
	}

	/// Write data to a range.
	///
	/// Returns the actual amount of bytes written.
	/// It may exit early if the necessary data is not cached (e.g. partial record write)
	pub async fn write(&self, offset: u64, mut data: &[u8]) -> Result<usize, Error<D>> {
		let root = self.root().await?;
		let len = u64::from(root.total_length);

		// Ensure all data fits.
		if offset >= len {
			return Ok(0);
		} else if offset + u64::try_from(data.len()).unwrap() >= len {
			data = &data[..usize::try_from(len - offset).unwrap()];
		}

		let Some(range) = calc_range(self.max_record_size(), offset, data.len()) else { return Ok(0) };
		let (first_offset, last_offset) =
			calc_record_offsets(self.max_record_size(), offset, data.len());

		if range.start() == range.end() {
			// We need to slice one record twice
			let b = self.get(0, *range.start()).await?;
			let mut b = b.get_mut().await?;
			let b = &mut b.data;
			let min_len = data.len().max(b.len());
			b.resize(min_len, 0);
			b[first_offset..last_offset].copy_from_slice(data);
			trim_zeros_end(b);
		} else {
			// We need to slice the first & last record once and operate on the others in full.
			let og_len = data.len();
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
				trim_zeros_end(b);
			}

			// Copy middle records |xxxxxxxx|
			for key in range {
				let d;
				(d, data) = data.split_at(1usize << self.max_record_size());
				let b = self.get(0, key).await?;
				let mut b = b.get_mut().await?;
				let b = &mut b.data;
				b.clear();
				b.extend_from_slice(d);
				trim_zeros_end(b);
			}

			// Copy end record |xxxx----|
			{
				let b = self.get(0, last_key).await?;
				let mut b = b.get_mut().await?;
				let b = &mut b.data;
				b[..last_offset].copy_from_slice(data);
				trim_zeros_end(b);
			}
		}

		Ok(data.len())
	}

	/// Read data from a range.
	///
	/// Returns the actual amount of bytes read.
	/// It may exit early if not all data is cached.
	pub async fn read(&self, offset: u64, mut buf: &mut [u8]) -> Result<usize, Error<D>> {
		let root = self.root().await?;
		let len = u64::from(root.total_length);

		// Ensure all data fits in buffer.
		if len >= offset {
			return Ok(0);
		} else if offset + u64::try_from(buf.len()).unwrap() >= len {
			buf = &mut buf[..usize::try_from(len - offset).unwrap()];
		}

		let Some(range) = calc_range(self.max_record_size(), offset, buf.len()) else { return Ok(0) };

		if range.start() == range.end() {
			// We need to slice one record twice
			todo!()
		} else {
			let og_len = buf.len();
			// We need to slice the first & last record once and operate on the others in full.
			todo!()
		}
	}

	/// Resize record tree.
	pub async fn resize(&self, new_len: u64) -> Result<(), Error<D>> {
		let root = self.root().await?;
		let len = u64::from(root.total_length);
		if new_len < len {
			self.shrink(root, new_len).await
		} else if new_len > len {
			self.grow(root, new_len).await
		} else {
			Ok(())
		}
	}

	/// Shrink record tree.
	async fn shrink(&self, root: Record, new_len: u64) -> Result<(), Error<D>> {
		debug_assert!(
			root.total_length > new_len,
			"new_len is equal or larger than cur len"
		);

		/// Destroy all records in a subtree recursively.
		#[async_recursion::async_recursion(?Send)]
		async fn destroy<D: Dev>(
			tree: &Tree<D>,
			depth: u8,
			offset: u64,
			record: &Record,
		) -> Result<(), Error<D>> {
			if depth > 0 {
				// We're a parent node, destroy children.
				let entry = tree.get(depth, offset).await?;
				for offt in 1..1usize << tree.max_record_size() {
					let rec = get_record(&entry.get().data, offt);
					let offt = (offset << tree.max_record_size()) + u64::try_from(offt).unwrap();
					destroy(tree, depth - 1, offt, &rec).await?;
				}
			}
			tree.cache.destroy(tree.id, depth, offset, record);
			Ok(())
		}

		// Reduce depth & remove completely out of range entries.
		let cur_depth = depth(self.max_record_size(), root.total_length.into());
		let new_depth = depth(self.max_record_size(), new_len);

		for d in (new_depth..cur_depth).rev() {
			// Get current root node.
			let entry = self.get(d + 1, 0).await?;
			// Destroy every subtree after the first child.
			for offt in 1..1 << self.max_record_size().to_raw() {
				let rec = get_record(&entry.get().data, offt);
				destroy(self, d, u64::try_from(offt).unwrap(), &root).await?;
			}
		}

		// Trim records on the right.
		todo!()
	}

	/// Grow record tree.
	async fn grow(&self, root: Record, new_len: u64) -> Result<(), Error<D>> {
		debug_assert!(
			root.total_length < new_len,
			"new_len is equal or smaller than cur len"
		);
		todo!()
	}

	/// The length of the record tree in bytes.
	pub async fn len(&self) -> Result<u64, Error<D>> {
		self.root().await.map(|rec| rec.total_length.into())
	}

	/// Get the root record of this tree.
	// TODO try to avoid boxing (which is what async_recursion does).
	// Can maybe be done in a clean way by abusing generics?
	// i.e. use "marker"/"tag" structs like ObjectTag and ListTag
	#[async_recursion::async_recursion(?Send)]
	pub async fn root(&self) -> Result<Record, Error<D>> {
		if self.id == OBJECT_LIST_ID {
			todo!("root of object list")
		} else {
			let list = Self::new(self.cache.clone(), OBJECT_LIST_ID);
			let mut record = Record::default();
			list.read(self.id * RECORD_SIZE, record.as_mut()).await?;
			Ok(record)
		}
	}

	/// Get a leaf cache entry.
	///
	/// It may fetch up to [`MAX_DEPTH`] of parent entries.
	// FIXME concurrent resizes will almost certainly screw something internally.
	// Maybe add a per object lock to the cache or something?
	async fn get(&self, target_depth: u8, offset: u64) -> Result<CacheRef<D>, Error<D>> {
		// This is very intentionally not recursive,
		// as you can't have async recursion without boxing.

		let rec_size = self.max_record_size().to_raw();

		let mut cur_depth = target_depth;
		let mut depth_offset_shift = rec_size;

		let root = self.root().await?;

		// Find the first parent or leaf entry that is present starting from a leaf
		// and work back downwards.

		let depth = depth(self.max_record_size(), root.total_length.into());
		assert!(target_depth <= depth);

		// FIXME we need to be careful with resizes while this task is running.
		// Perhaps lock object IDs somehow?

		// Go up
		// TODO has_entry doesn't check if an entry is already being fetched.
		while cur_depth < depth
			&& !self
				.cache
				.has_entry(self.id, cur_depth, offset >> depth_offset_shift)
		{
			cur_depth += 1;
			depth_offset_shift += rec_size - RECORD_SIZE_P2;
		}

		if cur_depth == target_depth {
			// The entry we need is already present.
			return Ok(self.cache.clone().lock_entry(
				self.id,
				cur_depth,
				offset >> depth_offset_shift,
			));
		}

		// Get first record to fetch.
		let mut record = Default::default();

		if cur_depth + 1 == depth {
			record = root;
		} else {
			let (offt, index) = divmod_p2(offset >> depth_offset_shift, rec_size - RECORD_SIZE_P2);
			let data = self
				.cache
				.get_entry(self.id, cur_depth, offset >> depth_offset_shift);
			let offt = index * mem::size_of::<Record>();
			record
				.as_mut()
				.copy_from_slice(&data.data[offt..offt + mem::size_of::<Record>()]);
		}

		// Fetch records until we can lock the one we need.
		let entry = loop {
			cur_depth -= 1;
			depth_offset_shift -= rec_size - RECORD_SIZE_P2;

			let entry = self
				.cache
				.clone()
				.fetch_entry(self.id, cur_depth, offset >> depth_offset_shift, &record)
				.await?;

			if cur_depth == target_depth {
				break entry;
			}

			let (offt, index) = divmod_p2(offset >> depth_offset_shift, rec_size - RECORD_SIZE_P2);
			let data = self
				.cache
				.get_entry(self.id, cur_depth, offset >> depth_offset_shift);
			let offt = index * mem::size_of::<Record>();
			record
				.as_mut()
				.copy_from_slice(&data.data[offt..offt + mem::size_of::<Record>()]);
		};

		Ok(entry)
	}

	/// Get the maximum record size.
	fn max_record_size(&self) -> MaxRecordSize {
		self.cache.max_record_size()
	}
}

/// Determine record range given an offset, record size and length.
///
/// Ranges are used for efficient iteration.
fn calc_range(
	record_size: MaxRecordSize,
	offset: u64,
	length: usize,
) -> Option<RangeInclusive<u64>> {
	// Avoid breaking stuff if offset == 0 (offset - 1 -> u64::MAX -> whoops)
	(length > 0).then(|| {
		// Determine range so we can iterate efficiently.
		// start & end are inclusive.
		let start_key = offset >> record_size;
		let end_key = (offset + u64::try_from(length).unwrap() - 1) >> record_size;
		start_key..=end_key
	})
}

/// Determine start & end offsets inside records.
fn calc_record_offsets(record_size: MaxRecordSize, offset: u64, length: usize) -> (usize, usize) {
	let mask = (1u64 << record_size) - 1;
	let start = offset & mask;
	let end = (offset + u64::try_from(length).unwrap()) & mask;
	(start.try_into().unwrap(), end.try_into().unwrap())
}

/// Cut off trailing zeroes from [`Vec`].
fn trim_zeros_end(vec: &mut Vec<u8>) {
	if let Some(i) = vec.iter().rev().position(|&x| x != 0) {
		vec.resize(vec.len() - i, 0);
	} else {
		vec.clear();
	}
	// TODO find a proper heuristic for freeing memory.
	if vec.capacity() / 2 <= vec.len() {
		vec.shrink_to_fit()
	}
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
		// TODO do it cleanly with ilog2_ceil
		while len > 1u64 << max_record_size {
			len >>= max_record_size.to_raw() - RECORD_SIZE_P2;
			depth += 1;
		}
		depth + 1
	}
}

/// Calculate `ceil(log2())`.
///
/// If `x` is 0, 0 is returned.
fn ilog2_ceil(x: u64) -> u8 {
	let Some(mut x) = x.checked_sub(1) else { return 0 };
	x |= x >> 1;
	x |= x >> 2;
	x |= x >> 4;
	x |= x >> 8;
	x |= x >> 16;
	x |= x >> 32;
	u8::try_from(x.trailing_ones()).unwrap()
}

/// Get a record from a slice of raw data.
fn get_record(data: &[u8], index: usize) -> Record {
	let mut record = Record::default();
	let offt = index * mem::size_of::<Record>();
	record
		.as_mut()
		.copy_from_slice(&data[offt..offt + mem::size_of::<Record>()]);
	record
}
