use {crate::Record, core::mem};

/// Get a record from a slice of raw data.
pub fn get_record(data: &[u8], index: usize) -> Record {
	let offt = index * mem::size_of::<Record>();

	let (start, end) = (offt, offt + mem::size_of::<Record>());
	let (start, end) = (start.min(data.len()), end.min(data.len()));

	let mut record = Record::default();
	record.as_mut()[..end - start].copy_from_slice(&data[start..end]);
	record
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
