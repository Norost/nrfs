use crate::{record::Record, storage::Storage, Error, RecordCache};

#[derive(Debug, Default)]
#[repr(transparent)]
pub struct RecordTree(pub Record);

struct NodeRef<'a>(&'a Record);

struct NodeMut<'a>(&'a mut Record);

impl RecordTree {
	/// # Note
	///
	/// `offset + data.len()` must not exceed [`Self::len`].
	pub fn read<S>(
		&self,
		sto: &mut RecordCache<S>,
		offset: u64,
		buf: &mut [u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		NodeRef(&self.0).read(self.len(), sto, offset, buf)
	}

	/// # Note
	///
	/// The record tree does *not* automatically grow. Use [`Self::resize`] to grow the tree.
	///
	/// `offset + data.len()` must not exceed [`Self::len`].
	pub fn write<S>(
		&mut self,
		sto: &mut RecordCache<S>,
		offset: u64,
		data: &[u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		let len = self.len();
		let ref_c = self.0.reference_count;
		NodeMut(&mut self.0).write(len, sto, offset, data)?;
		self.0.reference_count = ref_c;
		self.0.total_length = len.into();
		Ok(())
	}

	pub fn resize<S>(&mut self, sto: &mut RecordCache<S>, len: u64) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		if len < self.len() {
			self.shrink(sto, len)
		} else {
			self.grow(sto, len)
		}
	}

	fn shrink<S>(&mut self, sto: &mut RecordCache<S>, new_len: u64) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		let len = self.len();
		let ref_c = self.0.reference_count;
		NodeMut(&mut self.0).shrink(len, sto, new_len)?;
		self.0.reference_count = ref_c;
		self.0.total_length = new_len.into();
		Ok(())
	}

	fn grow<S>(&mut self, sto: &mut RecordCache<S>, len: u64) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		// Count how many levels need to be added.
		let (old_depth, _) = depth(self.len(), sto);
		self.0.total_length = len.into();
		let (new_depth, _) = depth(self.len(), sto);
		let ref_c = self.0.reference_count;
		for _ in old_depth..new_depth {
			let mut w = sto.write(&Record::default())?;
			set(&mut w, 0, &self.0);
			self.0.reference_count = 0;
			self.0 = w.finish()?;
		}
		self.0.reference_count = ref_c;
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
	pub fn read<S>(
		self,
		len: u64,
		sto: &mut RecordCache<S>,
		offset: u64,
		mut buf: &mut [u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
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
	pub fn write<S>(
		self,
		len: u64,
		sto: &mut RecordCache<S>,
		offset: u64,
		mut data: &[u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
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

	fn shrink<S>(self, len: u64, sto: &mut RecordCache<S>, new_len: u64) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		*self.0 = if let Some(chunk_size) = chunk_size(len, sto) {
			// Remove all records that would have zero length.
			let start = ((new_len + chunk_size - 1) / chunk_size) as usize;
			let end = (len / chunk_size) as usize;
			let mut w = sto.take(self.0)?;
			for i in start..end {
				NodeMut(&mut get(&w, i as _)).shrink(chunk_size, sto, 0)?;
			}
			w.resize(start * 64, 0);
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
	read_from(r, index as usize * 64, rec.as_mut());
	rec
}

fn set(w: &mut Vec<u8>, index: u32, rec: &Record) {
	write_to(w, index as usize * 64, rec.as_ref())
}

fn read_from(r: &[u8], offt: usize, buf: &mut [u8]) {
	if offt >= r.len() {
		buf.fill(0);
		return;
	}
	let i = r.len().min(offt + buf.len());
	let (l, h) = buf.split_at_mut(i - offt);
	l.copy_from_slice(&r[offt..][..l.len()]);
	h.fill(0);
}

fn write_to(w: &mut Vec<u8>, offt: usize, data: &[u8]) {
	if offt + data.len() > w.len() {
		w.resize(offt + data.len(), 0);
	}
	w[offt..][..data.len()].copy_from_slice(data);
}

/// Calculate the amount of data each chunk / child can hold.
///
/// If this returns `None` the record is a leaf and has no children.
fn chunk_size<S>(len: u64, sto: &RecordCache<S>) -> Option<u64>
where
	S: Storage,
{
	let (depth, lvl_shift) = depth(len, sto);
	depth
		.checked_sub(1)
		.map(|d| 1 << d * lvl_shift + sto.max_record_size.to_raw())
}

/// Calculate the depth and amount of records per record as a power of 2
fn depth<S>(len: u64, sto: &RecordCache<S>) -> (u8, u8)
where
	S: Storage,
{
	// The length in units of records.
	let max_rec_size = 1 << sto.max_record_size.to_raw();
	let len = (len + max_rec_size - 1) / max_rec_size;

	// mem::size_of<Record>() = 64 = 2^6
	let lvl_shift = sto.max_record_size.to_raw() - 6;

	let (mut lvl, mut depth) = (len, 0);
	// 0 = empty, 1 = not empty
	while lvl > 1 {
		lvl >>= lvl_shift;
		depth += 1;
	}

	(depth, lvl_shift)
}
