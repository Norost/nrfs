use {
	crate::{resource::Buf, Record},
	core::mem,
};

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

/// Cut off trailing zeroes from [`Buf`].
pub fn trim_zeros_end(vec: &mut impl Buf) {
	let i = vec
		.get()
		.iter()
		.rev()
		.position(|&x| x != 0)
		.unwrap_or(vec.len());
	vec.resize(vec.len() - i, 0);
	// TODO find a proper heuristic for freeing memory.
	if vec.capacity() / 2 <= vec.len() {
		vec.shrink()
	}
}
