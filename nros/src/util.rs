use {
	crate::{resource::Buf, Record},
	alloc::collections::btree_map,
	core::{future::Future, mem, pin::Pin},
};

/// Get a record from a slice of raw data.
///
/// Returns `None` if the index is completely out of range,
/// i.e. a zeroed record would be returned.
pub(crate) fn get_record(data: &[u8], index: usize) -> Option<Record> {
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
pub(crate) fn trim_zeros_end(vec: &mut impl Buf) {
	vec.resize(cutoff_zeros_end(vec.get()), 0);
	// TODO find a proper heuristic for freeing memory.
	if vec.capacity() / 2 <= vec.len() {
		vec.shrink()
	}
}

/// Find cutoff point beyond which are only zeros.
fn cutoff_zeros_end(data: &[u8]) -> usize {
	let i = data
		.iter()
		.rev()
		.position(|&x| x != 0)
		.unwrap_or(data.len());
	data.len() - i
}

/// Cut off trailing zeros from a byte slice
pub(crate) fn slice_trim_zeros_end(data: &[u8]) -> &[u8] {
	&data[..cutoff_zeros_end(data)]
}

/// Read from a slice, filling the remainder with zeros.
pub(crate) fn read(index: usize, buf: &mut [u8], data: &[u8]) {
	if index < data.len() {
		let data = &data[index..];
		if let Some(data) = data.get(..buf.len()) {
			buf.copy_from_slice(data);
		} else {
			buf[..data.len()].copy_from_slice(data);
			buf[data.len()..].fill(0)
		}
	} else {
		buf.fill(0)
	}
}

/// Box a future and erase its type.
pub(crate) fn box_fut<'a, T: Future + 'a>(fut: T) -> Pin<Box<dyn Future<Output = T::Output> + 'a>> {
	Box::pin(fut)
}

/// Calculate divmod with a power of two.
pub(crate) fn divmod_p2(offset: u64, pow2: u8) -> (u64, usize) {
	let mask = (1 << pow2) - 1;

	let index = offset & mask;
	let offt = offset >> pow2;

	(offt, index.try_into().unwrap())
}

pub(crate) trait BTreeMapExt<K, V> {
	fn occupied(&mut self, key: K) -> Option<btree_map::OccupiedEntry<'_, K, V>>;
}

impl<K: Ord + Eq, V> BTreeMapExt<K, V> for btree_map::BTreeMap<K, V> {
	fn occupied(&mut self, key: K) -> Option<btree_map::OccupiedEntry<'_, K, V>> {
		let btree_map::Entry::Occupied(e) = self.entry(key) else { return None };
		Some(e)
	}
}
