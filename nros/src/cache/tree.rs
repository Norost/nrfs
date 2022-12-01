use {
	super::CacheData,
	crate::Error,
	crate::MaxRecordSize,
	crate::DevSet,
	crate::Record,
	crate::Store,
	std::cell::{RefCell, RefMut},
	std::collections::btree_map::{BTreeMap, Entry},
	std::ops::RangeInclusive,
	std::rc::Rc,
};

/// The maximum depth of a record tree.
///
/// A record tree can contain up to 2^64 bytes of data.
/// The maximum record size is 8 KiB = 2^13 bytes.
/// Each record is 32 = 2^5 bytes large.
///
/// Ergo, maximum depth is `ceil((64 - 13) / (13 - 5)) = 7`
const MAX_DEPTH: u8 = 7;

/// Tree data, separated from values that can be derived elsewhere to save memory.
#[derive(Default, Debug)]
pub struct TreeData {
	/// Cached records.
	///
	/// Level (index) 0 is for data.
	/// Every level above are parent records.
	///
	/// Since the depth is at most 7.
	///
	/// Each key is the offset divided by the amount of data covered (i.e. accounting for depth).
	cache: [BTreeMap<u64, Vec<u8>>; MAX_DEPTH as usize],
	/// Length of the tree, in bytes.
	///
	/// This value is used to calculate the depth.
	len: u64,
	/// How much *uncached* data is still valid.
	///
	/// It allows avoiding redundantly fetching data.
	uncached_len: u64,
}

/// Implementation of record trees with caching.
///
/// This is meant to be reconstructed from [`TreeData`] ad-hoc.
#[derive(Clone, Copy, Debug)]
pub struct Tree<'a, D: Dev> {
	/// Tree data.
	pub data: Rc<RefCell<TreeData>>,
	/// The maximum record size allowed by the record store.
	pub max_record_size: MaxRecordSize,
	/// The root of the tree, which is normally stored elsewhere.
	pub root: Record,
	/// Backing store.
	pub store: Rc<Store<D>>,
}
*/

/// Implementation of a record tree.
pub struct Tree<D: Dev> {
	/// Underlying data handler.
	data: Rc<RefCell<CacheData<D>>>,
	/// ID of the object.
	id: u64,
}

impl Tree {
	/// Write data to a range.
	///
	/// Returns the actual amount of bytes written.
	/// It may exit early if the necessary data is not cached (e.g. partial record write)
	pub async fn write(&mut self, record_size: MaxRecordSize, offset: u64, data: &[u8]) -> Result<usize, OutOfRange> {
		let end = offset + u64::try_from(buf.len());
		if end >= self.len() {
			return Err(OutOfRange);
		}

		let Some(range) = calc_range(record_size, offset, data.len()) else { return 0 };

		// Offset in first record to copy from
		let (first_offset, last_offset) = calc_range(record_size, offset, data.len());

		if range.len() == 1 {
			// We need to slice one record twice
			let key = *range.start();
			let buf = self.get(&key).or_default();
			let min_len = data.len().max(buf.len());
			// TODO trim trailing zeroes
			buf.resize(min_len, 0);
			buf[first_offset..last_offset].copy_from_slice(data);
			Ok(data.len())
		} else {
			// We need to slice the first & last record once and operate on the others in full.
			let og_len = data.len();
			let mut range = range.into_iter();

			let first_key = range.next().unwrap();
			let last_key = range.next_back().unwrap();

			// Copy to first record |----xxxx|
			let d;
			(d, data) = data.split((1 << record_size.to_raw()) - first_key);
			let b = match self.cache.entry(&first_key) {
				Entry::Occupied(e) => e.get_mut(),
				Entry::Vacant(e) if self.key_inside_uncached(self.resize, first_key) => e.insert(Default::default()),
				Entry::Vacant(_) => return Ok(0), // We need to fetch data first.
			};
			b.resize(first_key, 0);
			b.extend_from_slice(remove_trailing_zeroes(d));

			// Copy middle records |xxxxxxxx|
			for key in range {
				let d;
				(d, data) = data.split(1 << record_size.to_raw());
				let b = self.cache.entry(&key).or_default();
				b.clear();
				b.extend_from_slice(remove_trailing_zeroes(d))
			}

			// Copy end record |xxxx----|
			let uncached_offset = self.uncached_len % (1 << record_size.to_raw());
			let buf = match self.cache.entry(&first_key) {
				Entry::Occupied(e) => e.get_mut(),
				// If there is no uncached data or it will be fully overwritten, don't bother.
				Entry::Vacant(e) if self.uncached_len <= end => {
					e.insert(Default::default())
				}
				Entry::Vacant(_) => return Ok(og_len - data.len()), // We need to fetch data first.
			};
			let min_len = data.len().max(buf.len());
			// TODO trim trailing zeroes
			buf.resize(min_len, 0);
			buf[..last_offset].copy_from_slice(data);

			Ok(og_len)
		}
	}

	/// Read data from a range.
	///
	/// Returns the actual amount of bytes read.
	/// It may exit early if not all data is cached.
	pub async fn read(&mut self, record_size: MaxRecordSize, offset: u64, mut buf: &mut [u8]) -> Result<usize, OutOfRange> {
		if offset + u64::try_from(buf.len()) >= self.len {
			return Err(OutOfRange);
		}

		let Some(range) = calc_range(record_size, offset, data.len()) else { return 0 };

		if range.len() == 1 {
			// We need to slice one record twice
			todo!()
		} else {
			let og_len = buf.len();
			// We need to slice the first & last record once and operate on the others in full.
			todo!()
		}
	}

	/// Resize record tree.
	pub fn resize(&mut self, record_size: MaxRecordSize, new_len: u64) {
		if self.len > new_len {
			// Remove out-of-range data.
			let remove_key = (new_len + (1 << record_size.to_raw()) - 1) >> record_size.to_raw();
			// TODO use remove_range or whatever if it is ever added to BTreeMap
			while let Some((&k, _)) = self.cache.range(remove_key..).next_back() {
				self.cache.remove(&k);
			}
			// Trim partially out of range record.
			let trim_key = new_len >> record_size.to_raw();
			let trim_len = usize::try_from(new_len - (trim_key << record_size.to_raw())).unwrap();
			if let Some(data) = self.cache.get_mut(&trim_key) {
				if data.len() > trim_len {
					data.resize(trim_len, 0);
				}
			}
		}

		self.len = new_len;
		self.uncached_len = self.uncached_len.min(self.len);
	}

	/// The length of the record tree in bytes.
	pub fn len(&self) -> u64 {
		self.data.len
	}

	/// Insert a record of data.
	/// 
	/// This completely overwrites other data that may have been written.
	///
	/// It also *does not* check if the size of the data is valid!
	pub fn insert(&mut self, record_size: MaxRecordSize, offset: u64, data: Vec<u8>, len: u32) {
		self.data.cache.insert(offset >> record_size.to_raw(), Entry { data, len });
	}

	/// Remove a record of data.
	pub fn remove(&mut self, record_size: MaxRecordSize, offset: u64) {
		self.data.cache.remove(&(offset >> record_size.to_raw()));
	}

	/// Check if a key is inside the uncached range or not.
	fn key_inside_uncached(&self, key: u64) -> bool {
		key << self.max_record_size.to_raw() < self.data.uncached_len
	}

	/// Calculate the depth of the record tree.
	fn depth(&self) -> u8 {
		// Round up to next power of two and calculate exponent.
		// i.e. x -> ceil(x) = 2^n -> n = log2(ceil(x))
		// Also account for overflow, i.e. calculate 2^n - 1 instead.

		let mut x = self.len();
		x |= x >> 1;
		x |= x >> 2;
		x |= x >> 4;
		x |= x >> 8;
		x |= x >> 16;
		x |= x >> 32;

		// TODO avoid div
		u8::try_from(x.trailing_ones()).unwrap().div_ceil(1 << (self.max_record_size.to_raw() - 5))
	}

	/// Get a leaf cache entry.
	///
	/// It may fetch up to [`MAX_DEPTH`] of parent entries.
	async fn get(self, offset: u64) -> Result<CacheEntry, Error<D>> {
		// This is very intentionally not recursive,
		// as you can't have async recursion without boxing.
		let mut cur_depth = 0;
		let mut depth_offset_shift = self.max_record_size.to_raw();
		
		// Find the first parent or leaf entry that is present starting from a leaf
		// and work back downwards.

		// Go up
		while cur_depth < self.depth() && !self.data.cache[usize::from(cur_depth)].contains(&(offset >> depth_offset_shift)) {
			cur_depth += 1;
			depth_offset_shift += self.max_record_size.to_raw() - 5;
		}

		// FIXME how do we deal with double fetch?

		// Work back down.
		// Fetch root record if necessary.
		let entry = if cur_depth == self.depth() {
			let data = self.store.read(&self.root).await?;
			self.data.cache[usize::from(cur_depth - 1)].insert(offset >> depth_offset_shift, data);
			CacheEntry {
				tree: self.data.clone(),
				offset: offset >> depth_offset_shift,
				depth: cur_depth,
			}
		} else {
		};
			CacheEntry {
				tree: self.data.clone(),
				offset: offset >> depth_offset_shift,
				depth: cur_depth,
			}

		while cur_depth > 0 {
		}

		Ok(entry)
	}
}

/// A cache entry.
///
/// It is a relatively cheap way to avoid lifetimes while helping ensure consistency.
/// 
/// Cache entries referenced by this structure cannot be removed until all corresponding
/// `CacheEntry`s are dropped.
///
/// # Note
///
/// `CacheEntry` is safe to hold across `await` points.
struct CacheEntry {
	tree: Rc<RefCell<TreeData>>,
	depth: u8,
	offset: u64,
}

impl CacheEntry {
	/// Get a mutable reference to the data.
	///
	/// # Note
	///
	/// The reference **must not** be held across `await` points!
	///
	/// # Panics
	///
	/// If something is already borrowing the underlying [`TreeData`].
	fn get_mut(&self) -> RefMut<Vec<u8>> {
		RefMut::map(self.tree.borrow_mut(), |tree| {
			tree.cache[usize::from(self.depth)]
				.get_mut(&self.offset)
				.expect("cache entry does not exist")
		})
	}
}

impl Drop for CacheEntry {
	fn drop(&mut self) {
		todo!("drop CacheEntry");
	}
}

/// Determine range given an offset, record size and length.
///
/// Ranges are used for efficient iteration.
fn calc_range(record_size: MaxRecordSize, offset: u64, length: usize) -> Option<RangeInclusive<u64>> {
	// Avoid breaking stuff if offset == 0 (offset - 1 -> u64::MAX -> whoops)
	(!data.is_empty()).then(|| {
		// Determine range so we can iterate efficiently.
		// start & end are inclusive.
		let start_key = offset >> record_size.to_raw();
		let end_key = (offset + u64::try_from(length).unwrap() - 1) >> record_size.to_raw();
		start_key..=end_key
	})
}

/// Determine start & end offsets inside records.
fn calc_record_offsets(record_size: MaxRecordSize, offset: u64, length: usize) -> (usize, usize) {
	let mask = (1 << record_size.to_raw()) - 1;
	let start = offset & mask;
	let end = (offset + u64::try_from(length).unwrap()) & mask;
	(start.try_into().unwrap(), end.try_into().unwrap())
}

/// Cut off trailing zeroes from slice.
fn remove_trailing_zeroes(slice: &[u8]) -> &[u8] {
	if let Some(i) = slice.iter().rev().position(|&x| x != 0) {
		&slice[..slice.len() - i]
	} else {
		&[]
	}
}

/// Error returned if data would be written past the boundaries of a tree.
#[derive(Debug)]
pub struct OutOfRange;
