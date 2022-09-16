use crate::{record::Record, storage::Storage, Error, RecordCache};

#[derive(Debug, Default)]
#[repr(transparent)]
pub struct RecordTree(pub Record);

impl RecordTree {
	pub fn read<S>(
		&self,
		sto: &mut RecordCache<S>,
		offset: u64,
		mut buf: &mut [u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		debug_assert!(
			offset + buf.len() as u64 <= u64::from(self.0.total_length),
			"{} + {} <= {}",
			offset,
			buf.len(),
			self.0.total_length
		);

		let (depth, lvl_shift) = self.depth_shift(sto);
		let chunk_size = 1 << depth * lvl_shift + sto.max_record_size_p2;

		let mut i = offset / chunk_size;
		let mut offt = offset % chunk_size;
		while !buf.is_empty() {
			let d;
			(d, buf) = buf.split_at_mut(buf.len().min((chunk_size - offset) as _));
			let rec = self.get(sto, i)?;
			if depth > 0 {
				rec.read(sto, offt, d)?
			} else {
				d.copy_from_slice(&sto.read(&rec.0)?[offt as _..][..d.len()])
			}
			offt = 0;
			i += 1;
		}
		Ok(())
	}

	pub fn write<S>(
		&mut self,
		sto: &mut RecordCache<S>,
		offset: u64,
		mut data: &[u8],
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		dbg!(self.len());
		let (depth, lvl_shift) = self.depth_shift(sto);
		let chunk_size = 1 << depth * lvl_shift + sto.max_record_size_p2;

		let end = offset + data.len() as u64;

		let mut i = offset / chunk_size;
		let mut offt = offset % chunk_size;
		while !data.is_empty() {
			let d;
			(d, data) = data.split_at(data.len().min((chunk_size - offset % chunk_size) as _));
			let mut rec = self.get(sto, i)?;
			if depth > 0 {
				rec.write(sto, offt, d)?;
			} else {
				let mut w = if offt == 0 && data.len() == 1 << sto.max_record_size_p2 {
					sto.write()
				} else {
					sto.modify(&rec.0)
				}?;
				write_to(&mut w, offt as _, d);
				rec = Self(w.finish()?);
			}
			self.set(sto, i, &rec)?;
			offt = 0;
			i += 1;
		}

		self.0.total_length = self.0.total_length.max(end.into());
		dbg!(self.len());

		Ok(())
	}

	pub fn truncate<S>(&mut self, sto: &mut RecordCache<S>, len: u64) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		dbg!(len);
		// FIXME adjust depth if necessary.
		assert_eq!(self.depth_shift(sto).0, 0);
		assert_eq!(u64::from(self.0.total_length), len);
		self.0.total_length = len.into();
		Ok(())
	}

	pub fn len(&self) -> u64 {
		self.0.total_length.into()
	}

	fn get<S>(&self, sto: &mut RecordCache<S>, index: u64) -> Result<Self, Error<S>>
	where
		S: Storage,
	{
		let mut rec = Record::default();
		let i = (index * 64) as _;
		sto.read(&self.0)?
			.get(i..i + 64)
			.map(|r| rec.as_mut().copy_from_slice(r));
		Ok(Self(rec))
	}

	fn set<S>(&mut self, sto: &mut RecordCache<S>, index: u64, rec: &Self) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		let mut w = sto.modify(&self.0)?;
		write_to(w.as_mut(), (index * 64) as _, rec.0.as_ref());
		self.0 = Record { total_length: self.0.total_length, ..w.finish()? };
		Ok(())
	}

	/// Calculate the depth and the amount of bits per index.
	fn depth_shift<S>(&self, sto: &RecordCache<S>) -> (u8, u8)
	where
		S: Storage,
	{
		let len = self.len() >> sto.max_record_size_p2;

		// mem::size_of<Record>() = 64 = 2^6
		let lvl_shift = sto.max_record_size_p2 - 6;

		let mut depth = {
			let (mut l, mut d) = (len, 0);
			while {
				l >>= lvl_shift;
				d += 1;
				l > 0
			} {}
			d - 1
		};

		(depth, lvl_shift)
	}
}

fn write_to(w: &mut Vec<u8>, offt: usize, data: &[u8]) {
	if let Some(w) = w.get_mut(offt..offt + data.len()) {
		w.copy_from_slice(data)
	} else {
		w.resize(offt, 0);
		w.extend_from_slice(data);
	}
}
