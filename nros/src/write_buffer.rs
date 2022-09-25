use {
	crate::{util, Error, RecordCache, RecordTree, Storage},
	core::mem,
	std::collections::{btree_map::Entry, BTreeMap},
};

/// A write buffer for record trees.
///
/// It caches writes to reduce/eliminate redundant compression and I/O.
pub struct WriteBuffer {
	/// Cache per record.
	/// Keys are offsets.
	/// Each value is a full record.
	records: BTreeMap<u64, Vec<u8>>,
	/// The current length of the record tree.
	total_length: u64,
	/// The minimum length the record tree had. Used to determine which records to erase.
	truncate_length: u64,
}

impl WriteBuffer {
	pub fn new(tree: &RecordTree) -> Self {
		Self {
			records: Default::default(),
			total_length: tree.len(),
			truncate_length: tree.len(),
		}
	}

	/// # Note
	///
	/// `offset + data.len()` must not exceed [`Self::len`].
	pub fn write<S>(
		&mut self,
		sto: &mut RecordCache<S>,
		tree: &RecordTree,
		mut offset: u64,
		mut data: &[u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		debug_assert!(
			offset + data.len() as u64 <= self.len(),
			"{} + {} <= {}",
			offset,
			data.len(),
			self.len()
		);

		let bs = 1 << sto.max_record_size.to_raw();

		while !data.is_empty() {
			let k = offset & !(bs as u64 - 1);
			let o = (offset & (bs as u64 - 1)) as usize;
			let d;
			fn trim_zeroes(d: &[u8]) -> &[u8] {
				let zero_padding = d.iter().rev().take_while(|c| **c == 0).count();
				&d[..d.len() - zero_padding]
			}
			if o == 0 && bs <= data.len() {
				// No need to fetch anything as we're thrashing the entire record.
				(d, data) = data.split_at(bs);
				let b = self.records.entry(k).or_default();
				b.clear();
				b.extend_from_slice(trim_zeroes(d));
			} else {
				(d, data) = data.split_at(data.len().min(bs - o));
				let f = |b: &mut Vec<u8>| {
					if o + d.len() < b.len() {
						util::write_to(b, o, d);
					} else {
						b.resize(o, 0);
						b.extend_from_slice(trim_zeroes(d));
					};
				};
				// Get the record from our caches or fetch it.
				match self.records.entry(k) {
					Entry::Occupied(mut e) => f(e.get_mut()),
					Entry::Vacant(e) if k >= self.truncate_length => {
						f(e.insert(Default::default()))
					}
					Entry::Vacant(e) => {
						let l = (self.truncate_length - k).min(bs as u64) as usize;
						let mut b = vec![0; l];
						tree.read(sto, k, &mut b)?;
						f(e.insert(b))
					}
				};
			}
			offset += d.len() as u64;
		}

		Ok(())
	}

	/// Read data into the buffer, accounting for unflushed writes.
	///
	/// # Note
	///
	/// `offset + data.len()` must not exceed [`Self::len`].
	pub fn read<S>(
		&mut self,
		sto: &mut RecordCache<S>,
		tree: &RecordTree,
		mut offset: u64,
		mut buf: &mut [u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		debug_assert!(
			offset + buf.len() as u64 <= self.len(),
			"{} + {} <= {}",
			offset,
			buf.len(),
			self.len()
		);

		let bs = 1 << sto.max_record_size.to_raw();

		while !buf.is_empty() {
			let k = offset & !(bs as u64 - 1);
			let o = (offset & (bs as u64 - 1)) as usize;
			let b;
			(b, buf) = buf.split_at_mut(buf.len().min(bs - o));
			if let Some(d) = self.records.get(&k) {
				util::read_from(d, o, b);
			} else if offset >= self.truncate_length {
				b.fill(0);
			} else {
				tree.read(sto, offset, b)?;
			}
			offset += b.len() as u64;
		}
		Ok(())
	}

	pub fn resize(&mut self, new_len: u64) {
		// Remove out of range records
		while let Some((&o, _)) = self.records.range(new_len..).next_back() {
			self.records.remove(&o);
		}

		// Truncate overlapping record
		if let Some((o, r)) = self.records.range_mut(..new_len).next_back() {
			if new_len < o + r.len() as u64 {
				r.resize((o - new_len) as _, 0);
			}
		}

		self.total_length = new_len;
		self.truncate_length = self.truncate_length.min(new_len);
	}

	/// Flush the buffer. This clears all cached data.
	pub fn flush<S>(
		&mut self,
		sto: &mut RecordCache<S>,
		tree: &mut RecordTree,
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		tree.resize(sto, self.truncate_length)?;
		tree.resize(sto, self.total_length)?;
		let mut it = self.records.iter_mut().map(|(o, r)| (*o, mem::take(r)));
		tree.insert_records(sto, &mut it)?;
		self.records.clear();
		self.truncate_length = self.total_length;
		Ok(())
	}

	pub fn len(&self) -> u64 {
		self.total_length
	}
}
