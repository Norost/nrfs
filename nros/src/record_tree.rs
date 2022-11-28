use {
	crate::{
		record::Record,
		util::{read_from, write_to},
		Error, MaxRecordSize,
	},
	core::mem,
	core::iter::Peekable,
};

/// Helper for handling record tree logic.
///
/// It does not perform I/O directly.
/// Instead, requests are delegated to a callback which then performs I/O as appropriate.
#[derive(Debug, Default)]
#[repr(transparent)]
pub struct RecordTree(pub Record);

struct NodeRef<'a>(&'a Record);

struct NodeMut<'a>(&'a mut Record);

/// Type of record to be fetched,
/// either a leaf with data or a parent with records pointing to other (record) nodes.
///
/// There are 5 levels.
/// The first and lowest is always for data.
///
pub enum Fetch<'a> {
	Data { offset: u64, record: &'a Record },
	Level0 { record: &'a Record },
	Level1 { record: &'a Record },
	Level2 { record: &'a Record },
	Level3 { record: &'a Record },
}

impl RecordTree {
	/// Fetch records.
	///
	/// # Note
	///
	/// `offset + data.len()` must not exceed [`Self::len`].
	pub fn fetch<I, F, E>(
		&self,
		iter: I,
		max_record_size: MaxRecordSize,
		f: F,
	) -> Result<(), E>
	where
		I: IntoIterator<Item = u64>,
		F: for<'b> FnMut(Fetch<'b>) -> Result<&[u8], E>
	{
		NodeRef(&self.0).read(self.len(), iter.into_iter().peekable(), f)
	}

	/// Insert records, completely overwriting the previous records.
	///
	/// The records *must* be sorted.
	pub fn store<I, F, E>(
		&mut self,
		iter: I,
		max_record_size: MaxRecordSize,
		f: F,
	) -> Result<(), E>
	where
		I: IntoIterator<Item = (u64, &[u8])>,
		F: for<'b> FnMut(Fetch<'b>) -> Result<&mut Vec<u8>, E>,
	{
		let (len, ref_c) = (self.0.total_length, self.0.references);
		let r = NodeMut(&mut self.0).store(self.len(), iter.into_iter().peekable(), f);
		(self.0.total_length, self.0.references) = (len, ref_c);
		r
	}

	fn shrink<F, E>(
		&mut self,
		new_len: u64,
		max_record_size: MaxRecordSize,
		mut erase: F,
	) -> Result<(), E>
	where
		F: FnMut(u64),
	{
		let len = self.len();
		let ref_c = self.0.references;
		NodeMut(&mut self.0).shrink(len, new_len, max_record_size, &mut erase)?;
		self.0.references = ref_c;
		self.0.total_length = new_len.into();
		Ok(())
	}

	fn grow<F, E>(
		&mut self,
		len: u64,
		max_record_size: MaxRecordSize,
		f: F,
	) -> Result<(), E>
	where
		F: for<'b> FnMut(Fetch<'b>) -> Result<&mut Vec<u8>, E>,
	{
		// Count how many levels need to be added.
		let (old_depth, _) = depth(self.len(), sto.max_record_size);
		self.0.total_length = len.into();
		let (new_depth, _) = depth(self.len(), sto.max_record_size);
		let ref_c = self.0.references;
		for _ in old_depth..new_depth {
			let mut w = sto.write(&Record::default())?;
			set(&mut w, 0, &self.0);
			self.0.references = 0.into();
			self.0 = w.finish()?;
		}
		self.0.references = ref_c;
		self.0.total_length = len.into();
		Ok(())
	}

	pub fn len(&self) -> u64 {
		self.0.total_length.into()
	}
}

impl NodeRef<'_> {
	/// # Note
	///
	/// `offset + data.len()` must not exceed [`Self::len`].
	pub fn read<D>(
		self,
		len: u64,
		sto: &mut Store<D>,
		offset: u64,
		mut buf: &mut [u8],
	) -> Result<(), Error<D>>
	where
		D: Dev,
	{
		debug_assert!(
			offset + buf.len() as u64 <= len,
			"{} + {} <= {}",
			offset,
			buf.len(),
			len
		);

		if let Some(chunk_size) = chunk_size(len, sto) {
			let mut i = offset / chunk_size;
			let mut offt = offset % chunk_size;
			let r = sto.take(self.0)?;
			while !buf.is_empty() {
				let d;
				let rem = chunk_size - offt;
				(d, buf) = buf.split_at_mut(buf.len().min(rem as _));
				NodeRef(&get(&r, i as _)).read(chunk_size, sto, offt, d)?;
				offt = 0;
				i += 1;
			}
			sto.insert_clean(self.0, r)?
		} else {
			let r = sto.read(self.0)?;
			read_from(&r, offset as _, buf);
		}
		Ok(())
	}
}

impl NodeMut<'_> {
	/// # Note
	///
	/// The record tree does *not* automatically grow. Use [`Self::resize`] to grow the tree.
	///
	/// `offset + data.len()` must not exceed [`Self::len`].
	pub fn store<D>(
		self,
		len: u64,
		sto: &mut Store<D>,
		offset: u64,
		mut data: &[u8],
	) -> Result<(), Error<D>>
	where
		D: Dev,
	{
		debug_assert!(
			offset + data.len() as u64 <= len,
			"{} + {} <= {}",
			offset,
			data.len(),
			len
		);

		*self.0 = if let Some(chunk_size) = chunk_size(len, sto) {
			let mut i = offset / chunk_size;
			let mut offt = offset % chunk_size;
			let mut w = sto.take(self.0)?;
			while !data.is_empty() {
				let d;
				let rem = chunk_size - offt;
				(d, data) = data.split_at(data.len().min(rem as _));
				let mut rec = get(&w, i as _);
				NodeMut(&mut rec).write(chunk_size, sto, offt, d)?;
				set(&mut w, i as _, &rec);
				offt = 0;
				i += 1;
			}
			sto.insert(self.0, w)
		} else {
			let mut w = if offset == 0 && data.len() >= len as _ {
				sto.write(self.0)?
			} else {
				sto.modify(self.0)?
			};
			write_to(&mut w, offset as _, data);
			w.finish()
		}?;

		Ok(())
	}

	fn shrink<F, E>(
		self,
		len: u64,
		new_len: u64,
		max_record_size: MaxRecordSize,
		erase: &mut F
	) -> Result<(), E>
	where
		F: FnMut(u64),
	{
		*self.0 = if let Some(chunk_size) = chunk_size(len, max_record_size) {
			// Remove all records that would have zero length.
			let start = ((new_len + chunk_size - 1) / chunk_size) as usize;
			let end = (len / chunk_size) as usize;
			let mut w = sto.take(self.0)?;
			for i in start..end {
				NodeMut(&mut get(&w, i as _)).shrink(chunk_size, max_record_size, 0, erase)?;
			}
			w.resize(start * mem::size_of::<Record>(), 0);
			// Resize tail record, if necessary.
			let tail = (new_len / chunk_size) as u32;
			let clen = new_len % chunk_size;
			if clen != 0 {
				let mut rec = get(&w, tail);
				NodeMut(&mut rec).shrink(clen, sto, clen)?;
				set(&mut w, tail, &rec);
			}
			// If the tail is 0, the depth can be reduced.
			if tail == 0 {
				sto.write(self.0)?.finish()?;
				get(&w, tail)
			} else {
				sto.insert(self.0, w)?
			}
		} else {
			if new_len == 0 {
				sto.write(self.0)?
			} else {
				let mut w = sto.modify(&self.0)?;
				w.resize(new_len as _, 0);
				w
			}
			.finish()?
		};
		Ok(())
	}
}

fn get(r: &[u8], index: u32) -> Record {
	let mut rec = Record::default();
	read_from(r, index as usize * mem::size_of::<Record>(), rec.as_mut());
	rec
}

fn set(w: &mut Vec<u8>, index: u32, rec: &Record) {
	write_to(w, index as usize * mem::size_of::<Record>(), rec.as_ref())
}

/// Calculate the amount of data each chunk / child can hold.
///
/// If this returns `None` the record is a leaf and has no children.
fn chunk_size(len: u64, max_record_size: MaxRecordSize) -> Option<u64> {
	let (depth, lvl_shift) = depth(len, max_record_size);
	depth
		.checked_sub(1)
		.map(|d| 1 << d * lvl_shift + max_record_size.to_raw())
}

/// Calculate the depth and amount of records per record as a power of 2
fn depth(len: u64, max_record_size: MaxRecordSize) -> (u8, u8) {
	// Round up to record size
	let max_rec_size = 1 << max_record_size.to_raw();
	let len = (len + max_rec_size - 1) & !(max_rec_size - 1);

	let lvl_shift = max_record_size.to_raw() - mem::size_of::<Record>().trailing_zeros() as u8;

	let (mut lvl, mut depth) = (len, 0);
	while lvl > max_rec_size {
		lvl >>= lvl_shift;
		depth += 1;
	}

	(depth, lvl_shift)
}

#[cfg(test)]
mod test {
	use super::*;

	const RECS_PER_1K: u64 = 1024 / mem::size_of::<Record>() as u64;

	#[test]
	fn depth_0_min() {
		assert_eq!(depth(0, MaxRecordSize::K1), (0, 5));
	}

	#[test]
	fn depth_0() {
		assert_eq!(depth(1000, MaxRecordSize::K1), (0, 5));
	}

	#[test]
	fn depth_0_max() {
		assert_eq!(depth(1024, MaxRecordSize::K1), (0, 5));
	}

	#[test]
	fn depth_1_min() {
		assert_eq!(depth(1025, MaxRecordSize::K1), (1, 5));
	}

	#[test]
	fn depth_2_min() {
		assert_eq!(depth(1024 * RECS_PER_1K + 1, MaxRecordSize::K1), (2, 5));
	}

	#[test]
	fn depth_2_min2() {
		assert_eq!(
			depth(1024 * (RECS_PER_1K * 2 - 1), MaxRecordSize::K1),
			(2, 5)
		);
	}

	#[test]
	fn depth_2_min3() {
		assert_eq!(
			depth(1024 * (RECS_PER_1K * 2 - 1) + 1, MaxRecordSize::K1),
			(2, 5)
		);
	}

	#[test]
	fn depth_2_max() {
		assert_eq!(
			depth(1024 * RECS_PER_1K * RECS_PER_1K, MaxRecordSize::K1),
			(2, 5)
		);
	}
}
