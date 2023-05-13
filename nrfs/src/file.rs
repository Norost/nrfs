use std::ops::Deref;

use crate::Item;

use {
	crate::{
		dir::{Dir, Kv},
		Dev, Error, ItemKey,
	},
	core::fmt,
};

/// How many multiples of the block size a file should be before it is unembedded.
///
/// Waste calculation: `waste = 1 - blocks / (blocks + 1)`
///
/// Some factors for reference:
///
/// +--------+------------------------------+--------------------+-------------------+
/// | Factor | Maximum waste (Uncompressed) | Maximum size (512) | Maximum size (4K) |
/// +========+==============================+====================+===================+
/// |      1 |                          50% |                512 |                4K |
/// +--------+------------------------------+--------------------+-------------------+
/// |      2 |                          33% |                 1K |                8K |
/// +--------+------------------------------+--------------------+-------------------+
/// |      3 |                          25% |               1.5K |               12K |
/// +--------+------------------------------+--------------------+-------------------+
/// |      4 |                          20% |                 2K |               16K |
/// +--------+------------------------------+--------------------+-------------------+
/// |      5 |                        16.6% |               2.5K |               20K |
/// +--------+------------------------------+--------------------+-------------------+
///
/// * Maximum waste = how much data may be padding if stored as an object.
const EMBED_FACTOR: u64 = 4;

/// Helper structure for working with files.
#[derive(Debug)]
pub struct File<'a, D: Dev> {
	pub(crate) item: Item<'a, D>,
}

impl<'a, D: Dev> File<'a, D> {
	/// Read data.
	///
	/// The returned value indicates how many bytes were actually read.
	pub async fn read(&self, offset: u64, mut buf: &mut [u8]) -> Result<usize, Error<D>> {
		trace!("read {} (len: {})", offset, buf.len());
		if buf.is_empty() {
			return Ok(0);
		}

		let (mut kv, dat) = self.data().await?;
		let end = calc_end(offset, buf.len()).unwrap_or(u64::MAX);

		if offset >= dat.len() {
			return Ok(0);
		}
		if end > dat.len() {
			buf = &mut buf[..usize::try_from(dat.len() - offset).unwrap()];
		}

		match dat {
			Data::Object { id, .. } => {
				self.fs.get(id).read(offset, buf).await?;
			}
			Data::Embed { offset: offt, .. } => kv.read(offt + offset, buf).await?,
		}

		Ok(buf.len())
	}

	/// Write data.
	///
	/// The returned value indicates how many bytes were actually written.
	pub async fn write(&self, offset: u64, mut data: &[u8]) -> Result<usize, Error<D>> {
		trace!("write {} (len: {})", offset, data.len());
		assert!(!self.fs.read_only, "read only");
		if data.is_empty() {
			return Ok(0);
		}

		let end = calc_end(offset, data.len()).unwrap_or(u64::MAX);
		let (mut kv, dat) = self.data().await?;
		if offset >= dat.len() {
			return Ok(0);
		}
		if end > dat.len() {
			data = &data[..usize::try_from(dat.len() - offset).unwrap()];
		}

		match dat {
			Data::Object { id, .. } => {
				self.fs.get(id).write(offset, data).await?;
			}
			Data::Embed { offset: offt, .. } => kv.write(offt + offset, data).await?,
		}

		Ok(data.len())
	}

	/// Write an exact amount of data,
	/// growing the object if necessary.
	pub async fn write_grow(
		&self,
		offset: u64,
		data: &[u8],
	) -> Result<Result<(), LengthTooLong>, Error<D>> {
		trace!("write_grow {} (len: {})", offset, data.len());
		assert!(!self.fs.read_only, "read only");
		if data.is_empty() {
			return Ok(Ok(()));
		}

		let Some(end) = calc_end(offset, data.len()) else { return Ok(Err(LengthTooLong)) };
		if end > self.fs.storage.obj_max_len() {
			return Ok(Err(LengthTooLong));
		}

		let (mut kv, mut dat) = self.data().await?;

		if end <= dat.len() {
			match dat {
				Data::Object { id, .. } => {
					self.fs.get(id).write(offset, data).await?;
				}
				Data::Embed { offset: offt, .. } => kv.write(offt + offset, data).await?,
			}
		} else {
			match &mut dat {
				Data::Object { id, length, .. } => {
					self.fs.get(*id).write(offset, data).await?;
					*length = end;
				}
				Data::Embed { offset: offt, length, capacity, .. }
					if u64::from(*capacity) >= end =>
				{
					kv.write(*offt + offset, data).await?;
					*length = end.try_into().unwrap();
				}
				Data::Embed { offset: offt, length, capacity, .. }
					if self.embed_factor() >= end =>
				{
					let _dir_lock = self.fs.lock_dir_mut(self.key.dir).await;
					let keep_len = u64::from(*length).min(end).try_into().unwrap();
					let new_cap = (end * 3 / 2).min(u16::MAX.into());
					let mut buf = vec![0; keep_len];
					kv.read(*offt, &mut buf).await?;
					kv.dealloc(*offt, (*capacity).into()).await?;
					let o = kv.alloc(new_cap).await?;
					kv.write(o.get(), &buf).await?;
					kv.write(o.get() + offset, data).await?;
					*offt = o.get();
					*length = end.try_into().unwrap();
					*capacity = new_cap.try_into().unwrap();
				}
				&mut Data::Embed { offset: offt, length, capacity, is_sym } => {
					let _dir_lock = self.fs.lock_dir_mut(self.key.dir).await;
					let keep_len = u64::from(length).min(end).try_into().unwrap();
					let mut buf = vec![0; keep_len];
					kv.read(offt, &mut buf).await?;
					kv.dealloc(offt, capacity.into()).await?;
					let obj = self.fs.storage.create().await?;
					obj.write(0, &buf).await?;
					obj.write(offset, data).await?;
					dat = Data::Object { is_sym, id: obj.id(), length: end };
				}
			}
			self.set_data(kv, dat).await?;
		}
		Ok(Ok(()))
	}

	/// Resize the file.
	pub async fn resize(&self, new_len: u64) -> Result<Result<(), LengthTooLong>, Error<D>> {
		trace!("resize {}", new_len);
		assert!(!self.fs.read_only, "read only");

		if new_len > self.fs.storage.obj_max_len() {
			return Ok(Err(LengthTooLong));
		}

		let (mut kv, mut dat) = self.data().await?;
		if dat.len() == new_len {
			return Ok(Ok(()));
		}
		if dat.len() > new_len {
			match &mut dat {
				&mut Data::Object { id, is_sym, .. } if new_len == 0 => {
					self.fs.get(id).dealloc().await?;
					dat = Data::Embed { is_sym, offset: 0, length: 0, capacity: 0 };
				}
				Data::Object { id, length, .. } => {
					self.fs.get(*id).write_zeros(new_len, u64::MAX).await?;
					*length = new_len;
				}
				Data::Embed { offset, length, .. } => {
					kv.write_zeros(*offset + new_len, u64::from(*length) - new_len)
						.await?;
					*length = new_len.try_into().unwrap();
				}
			}
		} else {
			match &mut dat {
				Data::Object { length, .. } => *length = new_len,
				Data::Embed { length, capacity, .. } if u64::from(*capacity) >= new_len => {
					*length = new_len.try_into().unwrap()
				}
				Data::Embed { length, capacity, offset, .. } if self.embed_factor() >= new_len => {
					let _dir_lock = self.fs.lock_dir_mut(self.key.dir).await;
					let mut buf = vec![0; (*length).into()];
					kv.read(*offset, &mut buf).await?;
					kv.dealloc(*offset, (*capacity).into()).await?;
					let offt = kv.alloc(new_len).await?;
					kv.write(offt.get(), &buf).await?;
					*offset = offt.get();
					*length = new_len.try_into().unwrap();
					*capacity = new_len.try_into().unwrap();
				}
				&mut Data::Embed { length, capacity, offset, is_sym } => {
					let _dir_lock = self.fs.lock_dir_mut(self.key.dir).await;
					let mut buf = vec![0; length.into()];
					kv.read(offset, &mut buf).await?;
					kv.dealloc(offset, capacity.into()).await?;
					let obj = self.fs.storage.create().await?;
					obj.write(0, &buf).await?;
					dat = Data::Object { is_sym, id: obj.id(), length: new_len };
				}
			}
		}
		self.set_data(kv, dat).await?;
		Ok(Ok(()))
	}

	pub async fn is_embed(&self) -> Result<bool, Error<D>> {
		trace!("is_embed");
		let ty = &mut [0];
		self.dir().kv().read_user_data(self.key.tag, 0, ty).await?;
		Ok(matches!(ty[0] & 7, 4 | 5))
	}

	/// Create stub dir helper.
	///
	/// # Note
	///
	/// This doesn't set a valid parent directory & chunk.
	fn dir(&self) -> Dir<'a, D> {
		Dir::new(self.fs, ItemKey::INVAL, self.key.dir)
	}

	/// Determine the embed factor.
	fn embed_factor(&self) -> u64 {
		let embed_lim = EMBED_FACTOR << self.fs.block_size().to_raw();
		u64::from(u16::MAX).min(embed_lim)
	}

	async fn data(&self) -> Result<(Kv<'a, D>, Data), Error<D>> {
		let mut kv = self.dir().kv();
		let buf = &mut [0; 16];
		kv.read_user_data(self.key.tag, 0, buf).await?;
		Ok((kv, Data::from_raw(*buf)))
	}

	async fn set_data(&self, mut kv: Kv<'_, D>, data: Data) -> Result<(), Error<D>> {
		kv.write_user_data(self.key.tag, 0, &data.into_raw()).await
	}
}

impl<'a, D: Dev> Deref for File<'a, D> {
	type Target = Item<'a, D>;

	fn deref(&self) -> &Self::Target {
		&self.item
	}
}

enum Data {
	Object { is_sym: bool, id: u64, length: u64 },
	Embed { is_sym: bool, offset: u64, length: u16, capacity: u16 },
}

impl Data {
	fn from_raw(raw: [u8; 16]) -> Self {
		let (a, _) = raw.split_array_ref::<8>();
		let (_, b) = raw.rsplit_array_ref::<8>();
		let a = u64::from_le_bytes(*a);
		let b = u64::from_le_bytes(*b);

		let ty = a & 7;

		let is_sym = ty & 1 != 0; // 3, 5
		match ty {
			2 | 3 => Self::Object { is_sym, id: a >> 5, length: b },
			4 | 5 => Self::Embed {
				is_sym,
				offset: a >> 16,
				length: b as u16,
				capacity: (b >> 32) as u16,
			},
			_ => todo!("invalid ty"),
		}
	}

	fn into_raw(self) -> [u8; 16] {
		let (a, b);
		match self {
			Self::Object { is_sym, id, length } => {
				a = (2 | u64::from(is_sym)) | (id << 5);
				b = length;
			}
			Self::Embed { is_sym, offset, length, capacity } => {
				a = (4 | u64::from(is_sym)) | (u64::from(offset) << 16);
				b = u64::from(length) | u64::from(capacity) << 32;
			}
		}
		let mut raw = [0; 16];
		raw[..8].copy_from_slice(&a.to_le_bytes());
		raw[8..].copy_from_slice(&b.to_le_bytes());
		raw
	}

	fn len(&self) -> u64 {
		match self {
			Self::Object { length, .. } => *length,
			Self::Embed { length, .. } => u64::from(*length),
		}
	}
}

/// Error returned if the length is larger than supported.
#[derive(Clone, Debug)]
pub struct LengthTooLong;

impl fmt::Display for LengthTooLong {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		"length too long".fmt(f)
	}
}

impl core::error::Error for LengthTooLong {}

fn calc_end(offset: u64, len: usize) -> Option<u64> {
	let len = u64::try_from(len).ok()?;
	offset.checked_add(len)
}
