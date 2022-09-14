use crate::{record::Record, storage::Storage, Error, RecordCache};

#[derive(Debug, Default)]
#[repr(transparent)]
pub struct RecordTree(pub Record);

impl RecordTree {
	pub fn read<S>(
		&self,
		sto: &mut RecordCache<S>,
		offset: u64,
		buf: &mut [u8],
	) -> Result<usize, Error<S>>
	where
		S: Storage,
	{
		let rec = self.find_record(sto, offset)?;
		let offt = offset % (1 << sto.max_record_size_p2);
		let l = buf.len();
		buf.copy_from_slice(&sto.read(&rec)?[offt as _..][..l]);
		Ok(l)
	}

	pub fn write<S>(
		&mut self,
		sto: &mut RecordCache<S>,
		offset: u64,
		data: &[u8],
	) -> Result<usize, Error<S>>
	where
		S: Storage,
	{
		let rec = self.find_record(sto, offset)?;
		let offt = (offset % (1 << sto.max_record_size_p2)) as usize;
		let mut w = sto.modify(&rec)?;
		if w.len() < offt + data.len() {
			w.resize(offt, 0);
			w.extend_from_slice(data);
		} else {
			w[offt..][..data.len()].copy_from_slice(data);
		}
		let rec = w.finish()?;
		self.0.total_length = self.len().max(offset + data.len() as u64).into();
		self.update_record(sto, offset, rec)?;
		Ok(data.len())
	}

	fn find_record<S>(&self, sto: &mut RecordCache<S>, offset: u64) -> Result<Record, Error<S>>
	where
		S: Storage,
	{
		let offt = offset >> sto.max_record_size_p2;
		let len = self.len() >> sto.max_record_size_p2;
		if offt > len {
			return Ok(Record::default());
		}

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

		let mut rec = self.0;
		while depth > 0 {
			depth -= 1;
			let i = offt >> lvl_shift * depth;
			let r = sto.read(&rec)?;
			rec.as_mut().copy_from_slice(&r[(i * 64) as _..][..64]);
		}
		Ok(rec)
	}

	fn update_record<S>(
		&mut self,
		sto: &mut RecordCache<S>,
		offset: u64,
		record: Record,
	) -> Result<(), Error<S>>
	where
		S: Storage,
	{
		let offt = offset >> sto.max_record_size_p2;
		let len = self.len() >> sto.max_record_size_p2;
		assert!(offt <= len);

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

		/*
		let mut rec = self.0;
		while depth > 0 {
			depth -= 1;
			let i = offt >> lvl_shift * depth;
			let r = sto.read(&rec)?;
			rec.as_mut().copy_from_slice(&r[(i * 64) as _..][..64]);
		}
		*/
		self.0 = Record { total_length: self.0.total_length, ..record };
		Ok(())
	}

	pub fn len(&self) -> u64 {
		self.0.total_length.into()
	}
}
