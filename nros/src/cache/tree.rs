use {
	super::{Cache, CacheData, CacheRef, OBJECT_LIST_ID},
	crate::Error,
	crate::MaxRecordSize,
	crate::DevSet,
	crate::Record,
	crate::Store,
	crate::Dev,
	core::cell::{RefCell, RefMut},
	std::collections::btree_map::{BTreeMap, Entry},
	core::ops::RangeInclusive,
	std::rc::Rc,
	core::mem,
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
	pub async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, OutOfRange> {
		let end = offset + u64::try_from(data.len()).unwrap();
		if end >= self.len() {
			return Err(OutOfRange);
		}

		let mut data = self.cache.data.borrow_mut();

		let Some(range) = calc_range(self.cache.max_record_size(), offset, data.len()) else { return 0 };

		// Offset in first record to copy from
		let (first_offset, last_offset) = calc_range(self.cache.max_record_size(), offset, data.len());

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
	pub async fn read(&self, offset: u64, mut buf: &mut [u8]) -> Result<usize, Error<D>> {
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
	pub async fn resize(&mut self, new_len: u64) {
	}

	/// Shrink record tree.
	async fn shrink(&mut self, new_len: u64) -> Result<(), Error<D>> {
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

	/// Grow record tree.
	async fn grow(&self, new_len: u64) -> Result<(), Error<D>> {
		todo!()
	}

	/// The length of the record tree in bytes.
	pub async fn len(&self) -> Result<u64, Error<D>> {
		self.root().await.map(|rec| rec.total_length.into())
	}

	/// Get the root record of this tree.
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

	/// Calculate the depth of the record tree.
	async fn depth(&self) -> Result<u8, Error<D>> {
		// Round up to next power of two and calculate exponent.
		// i.e. x -> ceil(x) = 2^n -> n = log2(ceil(x))
		// Also account for overflow, i.e. calculate 2^n - 1 instead.

		let Some(mut x) = self.len().await?.checked_sub(1) else { return 0 };
		x |= x >> 1;
		x |= x >> 2;
		x |= x >> 4;
		x |= x >> 8;
		x |= x >> 16;
		x |= x >> 32;

		// TODO avoid div
		let rec_size = self.cache.max_record_size();
		Ok(u8::try_from(x.trailing_ones()).unwrap().div_ceil(1 << (rec_size.to_raw() - RECORD_SIZE_P2)))
	}

	/// Get a leaf cache entry.
	///
	/// It may fetch up to [`MAX_DEPTH`] of parent entries.
	async fn get(self, offset: u64) -> Result<CacheRef<D>, Error<D>> {
		// This is very intentionally not recursive,
		// as you can't have async recursion without boxing.

		let rec_size = self.max_record_size().to_raw();

		let mut cur_depth = 0;
		let mut depth_offset_shift = rec_size;
		
		// Find the first parent or leaf entry that is present starting from a leaf
		// and work back downwards.

		let depth = self.depth().await?;

		// FIXME we need to be careful with resizes while this task is running.
		// Perhaps lock object IDs somehow?

		// Go up
		// TODO has_entry doesn't check if an entry is already being fetched.
		while cur_depth < depth && !self.cache.has_entry(self.id, cur_depth, offset >> depth_offset_shift) {
			cur_depth += 1;
			depth_offset_shift += rec_size - RECORD_SIZE_P2;
		}

		if cur_depth == 0 {
			// The entry we need is already present.
			return Ok(self.cache.lock_entry(self.id, cur_depth, offset >> depth_offset_shift));
		}

		// Get first record to fetch.
		let mut record = Default::default();

		if cur_depth + 1 == depth {
			// TODO silly us is fetching the root twice now, previously with self.depth()
			// FIXME we risk having the depth becoming invalid this way.
			record = self.root().await?;
		} else {
			let (offt, index) = divmod_p2(offset >> depth_offset_shift, rec_size - RECORD_SIZE_P2);
			let data = self.cache.get_entry(self.id, cur_depth, offset >> depth_offset_shift);
			let offt = index * mem::size_of::<Record>();
			record.as_mut().copy_from_slice(&data[offt..offt + mem::size_of::<Record>()]);
		}

		// Fetch records until we can lock the one we need.
		let entry = loop {
			cur_depth -= 1;
			depth_offset_shift -= rec_size - RECORD_SIZE_P2;

			self.cache.fetch_entry(self.id, cur_depth, offset >> depth_offset_shift, &record).await?;

			if cur_depth == 0 {
				break self.cache.lock_entry(self.id, cur_depth, offset >> depth_offset_shift);
			}

			let (offt, index) = divmod_p2(offset >> depth_offset_shift, rec_size - RECORD_SIZE_P2);
			let data = self.cache.get_entry(self.id, cur_depth, offset >> depth_offset_shift);
			let offt = index * mem::size_of::<Record>();
			record.as_mut().copy_from_slice(&data[offt..offt + mem::size_of::<Record>()]);
		};

		Ok(entry)
	}

	/// Get the maximum record size.
	fn max_record_size(&self) -> MaxRecordSize {
		self.cache.max_record_size()
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
		while len > 1 << max_record_size.to_raw() {
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
