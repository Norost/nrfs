use {
	crate::{allocator::Allocator, Error, LoadError, Read as _, Record, Storage, Write as _},
	alloc::{
		collections::{BTreeMap, BTreeSet},
		vec::Vec,
	},
	core::{
		marker::PhantomData,
		ops::{Deref, DerefMut},
	},
};

pub struct RecordCache<S: Storage> {
	pub(super) storage: S,
	cache: BTreeMap<u64, Vec<u8>>,
	dirty: BTreeSet<u64>,
	pub(super) max_record_size_p2: u8,
	allocator: Allocator,
}

impl<S: Storage> RecordCache<S> {
	pub fn new(mut storage: S, max_record_size_p2: u8) -> Self {
		Self {
			allocator: Allocator::default(),
			storage,
			max_record_size_p2,
			cache: Default::default(),
			dirty: Default::default(),
		}
	}

	pub fn load(
		mut storage: S,
		max_record_size_p2: u8,
		alloc_log_lba: u64,
		alloc_log_len: u64,
	) -> Result<Self, LoadError<S>> {
		Ok(Self {
			allocator: Allocator::load(&mut storage, alloc_log_lba, alloc_log_len)?,
			storage,
			max_record_size_p2,
			cache: Default::default(),
			dirty: Default::default(),
		})
	}

	pub fn read<'a>(&'a mut self, record: &Record) -> Result<Read<'a, S>, Error<S>> {
		self.read_inner(record)
			.map(|data| Read { data: data.map_or(&[], |d| &**d), _marker: PhantomData })
	}

	pub fn modify<'a>(&'a mut self, record: &Record) -> Result<Write<'a, S>, Error<S>> {
		self.read_inner(record)?;
		self.free(record);
		let data = self.cache.remove(&record.lba.into()).unwrap_or_default();
		Ok(Write { data, cache: self })
	}

	pub fn write<'a>(&'a mut self, record: &Record) -> Result<Write<'a, S>, Error<S>> {
		self.free(record);
		let mut data = self.cache.remove(&record.lba.into()).unwrap_or_default();
		data.clear();
		Ok(Write { data, cache: self })
	}

	fn free(&mut self, record: &Record) {
		self.allocator.free(
			record.lba.into(),
			self.calc_block_count(record.length.into()) as _,
		);
	}

	pub fn finish_transaction(&mut self) -> Result<(u64, u64), Error<S>> {
		self.allocator.serialize_full(&mut self.storage)
	}

	fn read_inner<'a>(&'a mut self, record: &Record) -> Result<Option<&'a mut Vec<u8>>, Error<S>> {
		if record.length == 0 {
			return Ok(None);
		}
		debug_assert!(record.lba != 0);

		let lba = record.lba.into();
		let len = record.length.into();
		// FIXME the borrow checker erroneously thinks that the borrow is still active after
		// the return even though it is in a branch.
		/*
		if let Some(d) = self.cache.get_mut(&lba) {
			return Ok(d);
		}
		*/
		if self.cache.contains_key(&lba) {
			return Ok(Some(self.cache.get_mut(&lba).unwrap()));
		}

		let count = self.calc_block_count(len);
		let rd = self.storage.read(lba, count).map_err(Error::Storage)?;
		let mut v = Vec::with_capacity(1 << self.max_record_size_p2);
		record
			.unpack(&rd.get()[..len as _], &mut v, self.max_record_size_p2)
			.map_err(Error::RecordUnpack)?;
		Ok(Some(self.cache.entry(lba).or_insert(v)))
	}

	fn calc_block_count(&self, len: u32) -> usize {
		calc_block_count(len, self.storage.block_size_p2(), self.max_record_size_p2)
	}
}

pub struct Read<'a, S: Storage> {
	data: &'a [u8],
	_marker: PhantomData<S>,
}

pub struct Write<'a, S: Storage> {
	cache: &'a mut RecordCache<S>,
	data: Vec<u8>,
}

impl<S: Storage> Deref for Read<'_, S> {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.data
	}
}

impl<S: Storage> Deref for Write<'_, S> {
	type Target = Vec<u8>;

	fn deref(&self) -> &Self::Target {
		&self.data
	}
}

impl<S: Storage> DerefMut for Write<'_, S> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.data
	}
}

impl<'a, S: Storage> Write<'a, S> {
	pub fn finish(self) -> Result<Record, Error<S>> {
		let len = self.data.len() as u32;
		let blks = self.cache.calc_block_count(len);
		let lba = self
			.cache
			.allocator
			.alloc(blks as _, &self.cache.storage)
			.ok_or(Error::NotEnoughSpace)?;
		let bs_p2 = self.cache.storage.block_size_p2();
		let mut w = self
			.cache
			.storage
			.write(lba, blks)
			.map_err(Error::Storage)?;
		w.set_blocks(blks);
		let mut rec = Record::pack(&self.data, w.get_mut()).map_err(Error::RecordPack)?;
		rec.lba = lba.into();
		w.set_blocks(calc_block_count(
			rec.length.into(),
			bs_p2,
			self.cache.max_record_size_p2,
		));
		self.cache.cache.insert(lba, self.data);
		self.cache.dirty.insert(lba);
		Ok(rec)
	}
}

fn calc_block_count(len: u32, block_size_p2: u8, max_record_size_p2: u8) -> usize {
	debug_assert!(len <= 1 << max_record_size_p2, "{:?}", len);
	let bs = 1 << block_size_p2;
	((len + bs - 1) / bs) as _
}
