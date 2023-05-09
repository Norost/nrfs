use crate::{dir::Kv, Dir};

use {
	crate::{Error, Nrfs},
	alloc::borrow::Cow,
	core::fmt,
	nrkv::{Key, Tag},
	nros::Dev,
};

#[derive(Debug)]
pub struct ItemInfo<'n> {
	pub key: ItemKey,
	pub name: Cow<'n, Key>,
	pub ty: ItemTy,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ItemKey {
	pub(crate) dir: u64,
	pub(crate) tag: Tag,
}

impl ItemKey {
	pub(crate) const INVAL: Self =
		Self { dir: 0xdeaddeaddeaddead, tag: Tag::new(0xbeefbeef).unwrap() };
}

impl fmt::Debug for ItemKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		format_args!("{:#x}:{:#x}", self.dir, self.tag.get()).fmt(f)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ItemTy {
	Dir = 1,
	File = 2,
	Sym = 3,
	EmbedFile = 4,
	EmbedSym = 5,
}

impl ItemTy {
	pub fn from_raw(raw: u8) -> Option<Self> {
		Some(match raw {
			1 => Self::Dir,
			2 => Self::File,
			3 => Self::Sym,
			4 => Self::EmbedFile,
			5 => Self::EmbedSym,
			_ => return None,
		})
	}
}

#[derive(Debug)]
pub struct Item<'a, D: Dev> {
	pub(crate) fs: &'a Nrfs<D>,
	pub(crate) key: ItemKey,
}

impl<'a, D: Dev> Item<'a, D> {
	pub(crate) fn new(fs: &'a Nrfs<D>, key: ItemKey) -> Self {
		Self { fs, key }
	}

	async fn read_attr(&self) -> Result<(Kv<'a, D>, (u64, u16), Vec<u8>), Error<D>> {
		let mut kv = Dir::new(self.fs, ItemKey::INVAL, self.key.dir).kv();

		let a = &mut [0; 8];
		kv.read_user_data(self.key.tag, 16, a).await?;
		let a = u64::from_le_bytes(*a);
		let (offt, len) = (a >> 16, a as u16);

		let mut attr = vec![0; len.into()];
		kv.read(offt, &mut attr).await?;

		Ok((kv, (offt, len), attr))
	}

	async fn write_attr(
		&self,
		mut kv: Kv<'a, D>,
		(offt, len): (u64, u16),
		attr: Vec<u8>,
	) -> Result<(), Error<D>> {
		kv.dealloc(offt, len.into()).await?;
		let attr_len = u16::try_from(attr.len()).unwrap();
		let offt = kv.alloc(attr_len.into()).await?;
		kv.write(offt.get(), &attr).await?;
		let a = offt.get() << 16 | u64::from(attr_len);
		kv.write_user_data(self.key.tag, 16, &a.to_le_bytes())
			.await?;
		Ok(())
	}

	pub async fn attr_keys(&self) -> Result<Vec<Box<Key>>, Error<D>> {
		if self.key.dir == u64::MAX {
			return Ok(vec![]);
		}
		let _lock = self.fs.lock_item(self.key).await;
		let mut attr_map = self.fs.attr_map().await?;
		let (_, _, attr) = self.read_attr().await?;
		let mut attr = &*attr;
		let mut keys = vec![];
		while let Some((id, _)) = attr_next(&mut attr) {
			keys.push(attr_map.key(id).await?);
		}
		Ok(keys)
	}

	pub async fn attr(&self, key: &Key) -> Result<Option<Vec<u8>>, Error<D>> {
		if self.key.dir == u64::MAX {
			return Ok(None);
		}
		let _lock = self.fs.lock_item(self.key).await;
		let mut attr_map = self.fs.attr_map().await?;
		let Some(id) = attr_map.get_attr(key).await? else { return Ok(None) };
		let (_, _, attr) = self.read_attr().await?;
		let mut attr = &*attr;
		while let Some((i, val)) = attr_next(&mut attr) {
			if i == id {
				return Ok(Some(val.into()));
			}
		}
		Ok(None)
	}

	pub async fn set_attr(
		&self,
		key: &Key,
		value: &[u8],
	) -> Result<Result<(), SetAttrError>, Error<D>> {
		if self.key.dir == u64::MAX {
			return Ok(Err(SetAttrError::IsRoot));
		}

		let _lock = self.fs.lock_item_mut(self.key).await;
		let (kv, addr, mut attr) = self.read_attr().await?;
		if attr.len() + (8 + 1 + value.len().min(8)) > usize::from(u16::MAX) {
			return Ok(Err(SetAttrError::Full));
		}

		let mut attr_map = self.fs.attr_map().await?;
		let id = attr_map.ref_attr(key).await?;

		let mut a = &*attr;
		let mut start = 0;
		while let Some((i, _)) = attr_next(&mut a) {
			let end = attr.len() - a.len();
			if i == id {
				attr.drain(start..end);
				break;
			}
			start = end;
		}

		let mut id = id.get();
		while id > 0x7fff {
			let b = (id as u16 | 0x8000).to_le_bytes();
			attr.extend_from_slice(&b);
			id >>= 15;
		}
		let b = (id as u16).to_le_bytes();
		attr.extend_from_slice(&b);

		if value.len() < 255 {
			attr.push(value.len() as _);
		} else {
			let l = u32::try_from(value.len()).unwrap();
			attr.push(255);
			attr.extend_from_slice(&l.to_le_bytes());
		}
		attr.extend_from_slice(value);

		self.write_attr(kv, addr, attr).await?;

		Ok(Ok(()))
	}

	pub async fn del_attr(&self, key: &Key) -> Result<bool, Error<D>> {
		let mut attr_map = self.fs.attr_map().await?;
		let Some(id) = attr_map.get_attr(key).await? else { return Ok(false) };

		let _lock = self.fs.lock_item_mut(self.key).await;
		let (kv, addr, mut attr) = self.read_attr().await?;
		let mut a = &*attr;
		let mut start = 0;
		while let Some((i, _)) = attr_next(&mut a) {
			let end = attr.len() - a.len();
			if i == id {
				attr.drain(start..end);
				self.write_attr(kv, addr, attr).await?;
				attr_map.unref_attr(id).await?;
				return Ok(true);
			}
			start = end;
		}
		Ok(false)
	}

	pub async fn len(&self) -> Result<u64, Error<D>> {
		trace!("len");
		let buf = &mut [0; 16];
		if self.key.dir == u64::MAX {
			buf.copy_from_slice(&self.fs.storage.header_data()[..16]);
		} else {
			let _lock = self.fs.lock_item(self.key).await;
			let mut kv = Dir::new(self.fs, ItemKey::INVAL, self.key.dir).kv();
			kv.read_user_data(self.key.tag, 0, buf).await?;
		}
		let len = u64::from_le_bytes(buf[8..].try_into().unwrap());
		Ok(match ItemTy::from_raw(buf[0] & 7).unwrap() {
			ItemTy::Dir | ItemTy::EmbedFile | ItemTy::EmbedSym => len & 0xffff_ffff,
			ItemTy::File | ItemTy::Sym => len,
		})
	}

	pub fn key(&self) -> ItemKey {
		self.key
	}
}

#[derive(Clone, Debug)]
pub enum SetAttrError {
	Full,
	IsRoot,
}

impl fmt::Display for SetAttrError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Full => "full",
			Self::IsRoot => "is root",
		}
		.fmt(f)
	}
}

impl core::error::Error for SetAttrError {}

fn attr_next<'a>(attr: &mut &'a [u8]) -> Option<(nrkv::Tag, &'a [u8])> {
	if attr.is_empty() {
		return None;
	}
	let mut id = 0;
	for i in 0.. {
		let b;
		(b, *attr) = attr.split_array_ref::<2>();
		let b = u16::from_le_bytes(*b);
		id |= u64::from(b & 0x7fff) << i * 15;
		if b & 0x8000 == 0 {
			break;
		}
	}
	let id = id.try_into().unwrap();
	let len;
	(len, *attr) = attr.split_array_ref::<1>();
	let len = if len[0] < 255 {
		u32::from(len[0])
	} else {
		let len;
		(len, *attr) = attr.split_array_ref::<4>();
		u32::from_le_bytes(*len)
	};
	let val;
	(val, *attr) = attr.split_at(len.try_into().unwrap());
	Some((id, val))
}
