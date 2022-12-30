use {crate::Record, core::mem};

/// Get a record from a slice of raw data.
///
/// Returns `None` if the index is completely out of range,
/// i.e. a zeroed record would be returned.
pub fn get_record(data: &[u8], index: usize) -> Option<Record> {
	let offt = index * mem::size_of::<Record>();
	if offt >= data.len() {
		return None;
	}

	let (start, end) = (offt, offt + mem::size_of::<Record>());
	let (start, end) = (start.min(data.len()), end.min(data.len()));

	let mut record = Record::default();
	record.as_mut()[..end - start].copy_from_slice(&data[start..end]);
	Some(record)
}

/// Cut off trailing zeroes from [`Vec`].
pub fn trim_zeros_end(vec: &mut Vec<u8>) {
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
