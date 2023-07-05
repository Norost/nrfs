use {
	crate::{dir::Kv, Dir, Error, Nrfs, HDR_ROOT_OFFT},
	alloc::borrow::Cow,
	core::fmt,
	nrkv::{Key, Tag},
	nros::Dev,
};

pub(crate) const ITEM_LEN: u16 = 40;

const MODIFIED_OFFT: u16 = 16;
const ATTR_OFFT: u16 = 32;

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
		let mut kv = self.parent_kv();

		let a = &mut [0; 8];
		kv.read_user_data(self.key.tag, ATTR_OFFT, a).await?;
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
		kv.write_user_data(self.key.tag, ATTR_OFFT, &a.to_le_bytes())
			.await?;
		Ok(())
	}

	pub async fn attr_keys(&self) -> Result<Vec<Box<Key>>, Error<D>> {
		if self.key.dir == u64::MAX {
			return Ok(vec![]);
		}
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

		let (kv, addr, mut attr) = self.read_attr().await?;
		if attr.len() + (8 + 1 + value.len().min(8)) > usize::from(u16::MAX) {
			return Ok(Err(SetAttrError::Full));
		}

		let mut attr_map = self.fs.attr_map().await?;
		let id = 'new: {
			if let Some(id) = attr_map.get_attr(key).await? {
				let mut a = &*attr;
				let mut start = 0;
				while let Some((i, _)) = attr_next(&mut a) {
					let end = attr.len() - a.len();
					if i == id {
						attr.drain(start..end);
						break 'new id;
					}
					start = end;
				}
			}
			attr_map.ref_attr(key).await?
		};

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
			let mut kv = Dir::new(self.fs, ItemKey::INVAL, self.key.dir).kv();
			kv.read_user_data(self.key.tag, 0, buf).await?;
		}
		let len = u64::from_le_bytes(buf[8..].try_into().unwrap());
		Ok(match ItemTy::from_raw(buf[0] & 7).unwrap() {
			ItemTy::Dir | ItemTy::EmbedFile | ItemTy::EmbedSym => len & 0xffff_ffff,
			ItemTy::File | ItemTy::Sym => len,
		})
	}

	pub async fn modified(&self) -> Result<Modified, Error<D>> {
		let buf = &mut [0; 16];
		if self.key.dir == u64::MAX {
			buf.copy_from_slice(
				&self.fs.storage.header_data()[HDR_ROOT_OFFT..][MODIFIED_OFFT.into()..][..16],
			);
		} else {
			self.parent_kv()
				.read_user_data(self.key.tag, MODIFIED_OFFT, buf)
				.await?;
		}
		Ok(Modified {
			time: i64::from_le_bytes(buf[..8].try_into().unwrap()),
			gen: i64::from_le_bytes(buf[8..].try_into().unwrap()),
		})
	}

	pub async fn set_modified(&self, modified: Modified) -> Result<(), Error<D>> {
		let buf = &mut [0; 16];
		buf[..8].copy_from_slice(&modified.time.to_le_bytes());
		buf[8..].copy_from_slice(&modified.gen.to_le_bytes());
		if self.key.dir == u64::MAX {
			self.fs.storage.header_data_mut()[HDR_ROOT_OFFT..][MODIFIED_OFFT.into()..][..16]
				.copy_from_slice(buf);
			Ok(())
		} else {
			self.parent_kv()
				.write_user_data(self.key.tag, MODIFIED_OFFT, buf)
				.await
		}
	}

	pub async fn set_modified_time(&self, time: i64) -> Result<(), Error<D>> {
		if self.key.dir == u64::MAX {
			self.fs.storage.header_data_mut()[HDR_ROOT_OFFT..][MODIFIED_OFFT.into()..][..8]
				.copy_from_slice(&time.to_le_bytes());
			Ok(())
		} else {
			self.parent_kv()
				.write_user_data(self.key.tag, MODIFIED_OFFT, &time.to_le_bytes())
				.await
		}
	}

	pub async fn set_modified_gen(&self, gen: i64) -> Result<(), Error<D>> {
		if self.key.dir == u64::MAX {
			self.fs.storage.header_data_mut()[HDR_ROOT_OFFT..][(MODIFIED_OFFT + 8).into()..][..8]
				.copy_from_slice(&gen.to_le_bytes());
			Ok(())
		} else {
			self.parent_kv()
				.write_user_data(self.key.tag, MODIFIED_OFFT + 8, &gen.to_le_bytes())
				.await
		}
	}

	pub fn key(&self) -> ItemKey {
		self.key
	}

	fn parent_kv(&self) -> Kv<'a, D> {
		Dir::new(self.fs, ItemKey::INVAL, self.key.dir).kv()
	}

	pub(crate) async fn realloc(
		&self,
		to_dir: &Dir<'a, D>,
		item: &mut [u8; ITEM_LEN as _],
	) -> Result<(), Error<D>> {
		trace!("realloc {:?} -> {:#x}", self.key, to_dir.id);
		if self.key.dir == to_dir.id {
			return Ok(());
		}

		if matches!(item[0] & 7, 4 | 5) {
			let mut buf = vec![];
			let offt = u64::from_le_bytes(item[..8].try_into().unwrap()) >> 16;
			let len = u16::from_le_bytes(item[8..10].try_into().unwrap());

			buf.resize(len.into(), 0);
			self.parent_kv().read(offt, &mut buf).await?;
			self.parent_kv().dealloc(offt, len.into()).await?;

			let offt = to_dir.kv().alloc(len.into()).await?;
			to_dir.kv().write(offt.get(), &buf).await?;
			item[2..8].copy_from_slice(&offt.get().to_le_bytes()[..6]);
			item[12..14].copy_from_slice(&len.to_le_bytes());
		}

		let (_, (offt, len), buf) = self.read_attr().await?;
		self.parent_kv().dealloc(offt, len.into()).await?;

		let offt = to_dir.kv().alloc(len.into()).await?;
		to_dir.kv().write(offt.get(), &buf).await?;
		item[ATTR_OFFT.into()..][0..2].copy_from_slice(&len.to_le_bytes());
		item[ATTR_OFFT.into()..][2..8].copy_from_slice(&offt.get().to_le_bytes()[..6]);

		Ok(())
	}

	pub(crate) async fn destroy(self) -> Result<bool, Error<D>> {
		trace!("destroy {:?}", self.key);

		let mut kv = self.parent_kv();
		let buf = &mut [0; ITEM_LEN as _];
		kv.read_user_data(self.key.tag, 0, buf).await?;
		let a = u64::from_le_bytes(buf[..8].try_into().unwrap());
		let b = u64::from_le_bytes(buf[8..16].try_into().unwrap());
		match a & 7 {
			ty @ 1 | ty @ 2 | ty @ 3 => {
				if ty == 1 && b != 0 {
					return Ok(false);
				}
				self.fs.get(a >> 5).dealloc().await?;
			}
			4 | 5 => {
				let offt = a >> 16;
				let cap = (b >> 32) & 0xffff;
				kv.dealloc(offt, cap.into()).await?;
			}
			ty => panic!("invalid ty {}", ty),
		}
		let attr = u64::from_le_bytes(buf[ATTR_OFFT.into()..].try_into().unwrap());
		let (offt, len) = (attr >> 16, attr as u16);
		kv.dealloc(offt, len.into()).await?;
		Ok(true)
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

pub struct Modified {
	pub time: i64,
	pub gen: i64,
}

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
