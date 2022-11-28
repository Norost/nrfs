use {
	crate::MaxRecordSize,
	std::collections::btree_map::{BTreeMap, Entry},
	std::ops::RangeInclusive,
}

/// Cache for [`RecordTree`]s.
#[derive(Default)]
pub struct TreeCache {
	/// Cached records.
	///
	/// Each key is the offset divided by the record size.
	cache: BTreeMap<u64, Vec<u8>>,
	/// Length of the tree, in bytes.
	len: u64,
	/// How much *uncached* data is still valid.
	///
	/// It allows avoiding redundantly fetching data.
	uncached_len: u64,
}

impl TreeCache {
	/// Write data to a range.
	///
	/// Returns the actual amount of bytes written.
	/// It may exit early if the necessary data is not cached (e.g. partial record write)
	pub fn write(&mut self, record_size: MaxRecordSize, offset: u64, data: &[u8]) -> Result<usize, OutOfRange> {
		let end = offset + u64::try_from(buf.len());
		if end >= self.len {
			return Err(OutOfRange);
		}

		let Some(range) = calc_range(record_size, offset, data.len()) else { return 0 };

		// Offset in first record to copy from
		let (first_offset, last_offset) = calc_range(record_size, offset, data.len());

		if range.len() == 1 {
			// We need to slice one record twice
			let key = *range.start();
			let buf = self.cache.entry(&key).or_default();
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
	pub fn read(&mut self, record_size: MaxRecordSize, offset: u64, mut buf: &mut [u8]) -> Result<usize, OutOfRange> {
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
		self.len
	}

	/// Insert a record of data.
	/// 
	/// This completely overwrites other data that may have been written.
	///
	/// It also *does not* check if the size of the data is valid!
	pub fn insert(&mut self, record_size: MaxRecordSize, offset: u64, data: Vec<u8>, len: u32) {
		self.cache.insert(offset >> record_size.to_raw(), Entry { data, len });
	}

	/// Remove a record of data.
	pub fn remove(&mut self, record_size: MaxRecordSize, offset: u64) {
		self.cache.remove(&(offset >> record_size.to_raw()));
	}

	/// Check if a key is inside the uncached range or not.
	fn key_inside_uncached(&self, record_size: MaxRecordSize, key: u64) -> bool {
		key << record_size.to_raw() < self.uncached_len
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
