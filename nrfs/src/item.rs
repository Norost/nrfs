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
	pub name: Option<Cow<'n, Key>>,
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

	async fn get_attr(&self) -> Result<(Kv<'a, D>, Vec<u8>, (u64, u16), Vec<u8>), Error<D>> {
		let mut kv = Dir::new(self.fs, ItemKey::INVAL, self.key.dir).kv().await?;

		let a = &mut [0; 8];
		kv.read_user_data(self.key.tag, 16, a).await?;
		let a = u64::from_le_bytes(*a);
		let (offt, len) = (a >> 16, a as u16);

		let mut attr = vec![0; len.into()];
		kv.read(offt, &mut attr).await?;

		let count = attr.get(0).map_or(0, |&x| usize::from(x) + 1);
		let mut keyarr = vec![0; count * 2];
		kv.read(0, &mut keyarr).await?;
		Ok((kv, keyarr, (offt, len), attr))
	}

	pub async fn attr_keys(&self) -> Result<Vec<Box<Key>>, Error<D>> {
		let (mut kv, keyarr, _, attr) = self.get_attr().await?;
		let mut offt = 0;
		let mut keys = vec![];
		for (i, len) in keyarr.into_iter().step_by(2).enumerate() {
			let (u, v) = (i / 8, i % 8);
			if attr[1 + u] >> v & 1 != 0 {
				let mut key = vec![0; len.into()];
				kv.read(512 + offt, &mut key).await?;
				keys.push(key.into_boxed_slice().try_into().unwrap());
			}
			offt += u64::from(len);
		}
		Ok(keys)
	}

	pub async fn attr(&self, key: &Key) -> Result<Option<Vec<u8>>, Error<D>> {
		let (mut kv, keyarr, _, attr) = self.get_attr().await?;
		let count = keyarr.len() / 2;
		let mut k_offt = 512;
		let hash = kv.hash(key) as u8;

		let bits_len = (count + 7) / 8;
		let mut v_offt =
			1 + bits_len + (0..bits_len).fold(0, |s, i| s + attr[1 + i].count_ones() as usize);

		let mut c = 0;
		for (i, &[len, h]) in keyarr.as_chunks::<2>().0.iter().enumerate() {
			if test_bit(&attr[1..], i) {
				dbg!(&attr, &keyarr);
				let l = usize::from(attr[dbg!(1 + (keyarr.len() / 2 + 7) / 8) + c]);
				if len == key.len_u8() && h == hash {
					let mut k = vec![0; len.into()];
					kv.read(k_offt, &mut k).await?;
					if &**key == &*k {
						if l < 255 {
							return Ok(Some(attr[v_offt..v_offt + l].into()));
						} else {
							todo!()
						}
					}
				}
				v_offt += if l == 255 { 8 } else { l };
				c += 1;
			}
			k_offt += u64::from(len);
		}
		Ok(None)
	}

	pub async fn set_attr(
		&self,
		key: &Key,
		value: &[u8],
	) -> Result<Result<(), SetAttrError>, Error<D>> {
		let (mut kv, keyarr, (offt, len), attr) = self.get_attr().await?;
		let Some(i) = self.get_or_add_key(&mut kv, key, &keyarr).await?
			else { todo!("too many keys, try GC") };
		let value = if value.len() <= 32 {
			AttrVal::Short(value)
		} else {
			todo!()
		};
		let new_attr = insert_attr_at(&attr, i, value);
		drop(attr);
		if new_attr.len() <= usize::from(len) {
			kv.write(offt, &new_attr).await?;
		} else {
			kv.dealloc(offt, len.into()).await?;
			let len = u16::try_from(new_attr.len()).unwrap();
			let offt = kv.alloc(len.into()).await?;
			let a = offt.get() << 16 | u64::from(len);
			kv.write_user_data(self.key.tag, 16, &a.to_le_bytes())
				.await?;
			kv.write(offt.get(), &new_attr).await?;
		}
		Ok(Ok(()))
	}

	async fn get_or_add_key(
		&self,
		kv: &mut Kv<'a, D>,
		key: &Key,
		keyarr: &[u8],
	) -> Result<Option<u8>, Error<D>> {
		let mut k_offt = 512;
		let hash = kv.hash(key) as u8;
		for (i, &[len, h]) in keyarr.as_chunks::<2>().0.iter().enumerate() {
			if len == key.len_u8() && h == hash {
				let mut k = vec![0; len.into()];
				kv.read(k_offt, &mut k).await?;
				if &**key == &*k {
					return Ok(Some(i.try_into().unwrap()));
				}
			}
			k_offt += u64::from(len);
		}
		if k_offt + u64::from(key.len_u8()) > KEYSTR_CAP {
			return Ok(None);
		}
		let Ok(i) = u8::try_from(keyarr.len() / 2) else { return Ok(None) };
		kv.write(k_offt, key).await?;
		kv.write(u64::from(i) * 2, &[key.len_u8(), hash]).await?;
		Ok(Some(i))
	}
}

const KEYSTR_CAP: u64 = (1 << 13) - (1 << 5) - 8;

#[derive(Clone, Debug)]
pub enum SetAttrError {}

enum AttrVal<'a> {
	Short(&'a [u8]),
	Long { offset: u64, len: u16 },
}

fn insert_attr_at(attr: &[u8], at: u8, value: AttrVal<'_>) -> Vec<u8> {
	let mut v_offt = 0;
	let mut buf = vec![];

	let (cur_count, attr) = if let Some((&max, attr)) = attr.split_first() {
		buf.push(max.max(at));
		(usize::from(max) + 1, attr)
	} else {
		buf.push(at);
		(0, attr)
	};
	let new_count = cur_count.max(usize::from(at) + 1);

	let bits_cur_len = (cur_count + 7) / 8;
	let bits_new_len = (new_count + 7) / 8;
	let (bits, attr) = attr.split_at(bits_cur_len);
	buf.extend_from_slice(bits);
	buf.resize(1 + bits_new_len, 0);
	buf[1 + usize::from(at / 8)] |= 1 << at % 8;

	let test = |i| test_bit(bits, i);
	let mut i @ mut c = 0;
	while i < usize::from(at) {
		if test(i) {
			let l = usize::from(attr[c]);
			v_offt += if l == 255 { 8 } else { l };
			c += 1;
		}
		i += 1;
	}

	let prev_len = usize::from(if test(i) { attr[c] } else { 0 });
	let prev_len = if prev_len == 255 { 8 } else { prev_len };

	let mut d = c;
	while i <= cur_count {
		d += usize::from(test(i));
		i += 1;
	}

	buf.extend_from_slice(&attr[..c]);
	buf.push(if let AttrVal::Short(v) = value {
		v.len() as _
	} else {
		255
	});
	c += usize::from(test(i));
	buf.extend_from_slice(&attr[c..d + v_offt]);
	match value {
		AttrVal::Short(v) => buf.extend_from_slice(v),
		AttrVal::Long { offset, len } => {
			buf.extend_from_slice(&len.to_le_bytes());
			buf.extend_from_slice(&offset.to_le_bytes()[..6]);
		}
	}
	buf.extend_from_slice(&attr[d + v_offt + prev_len..]);

	buf
}

fn test_bit(bits: &[u8], i: usize) -> bool {
	let (u, v) = (i / 8, i % 8);
	*bits.get(u).unwrap_or(&0) >> v & 1 != 0
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn attr_insert_short() {
		let a = insert_attr_at(&[], 0, AttrVal::Short(b"hello"));
		assert_eq!(&[0, 1 << 0, 5, b'h', b'e', b'l', b'l', b'o'], &*a);
	}

	#[test]
	fn attr_insert_long() {
		let a = insert_attr_at(&[], 0, AttrVal::Long { offset: 0xc0dedeadbeef, len: 42 });
		assert_eq!(
			&[0, 1 << 0, 255, 42, 0, 0xef, 0xbe, 0xad, 0xde, 0xde, 0xc0],
			&*a
		);
	}

	#[test]
	fn attr_insert_mid() {
		let a = insert_attr_at(&[], 20, AttrVal::Short(b"hello"));
		assert_eq!(&[20, 0, 0, 1 << 4, 5, b'h', b'e', b'l', b'l', b'o'], &*a);
	}

	#[test]
	fn attr_insert_multi() {
		let a = insert_attr_at(&[], 0, AttrVal::Short(b"hello"));
		let a = insert_attr_at(&a, 20, AttrVal::Short(b"world"));
		let a = insert_attr_at(&a, 3, AttrVal::Short(b"big"));
		let mut t = vec![0u8; 0];
		t.extend(&[20, 1 << 0 | 1 << 3, 0, 1 << 4]);
		t.extend(&[5, 3, 5]);
		t.extend(b"hello");
		t.extend(b"big");
		t.extend(b"world");
		assert_eq!(t, a);
	}
}
