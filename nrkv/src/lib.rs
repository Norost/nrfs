//#![no_std]
#![deny(unused_must_use)]
#![feature(const_waker, generic_arg_infer, never_type, split_array)]

#[cfg(test)]
mod test;

extern crate alloc;

use {
	alloc::boxed::Box,
	core::{
		future::{self, Future},
		hash::Hasher,
		mem,
		pin::Pin,
	},
	rand_core::{CryptoRng, RngCore},
	siphasher::sip128::{Hasher128, SipHasher13},
};

const HEADER_SIZE: u64 = 64;
const HAMT_ENTRY_SIZE: u64 = 6;
const HAMT_ROOT_LEN: u64 = 4096;
const HAMT_CHILD_LEN: u64 = 16;

type Tag = core::num::NonZeroU64;

pub trait Store {
	type Error;

	fn read(
		&mut self,
		offset: u64,
		buf: &mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>>>>;
	fn write(
		&mut self,
		offset: u64,
		data: &[u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>>>>;
	fn write_zeros(
		&mut self,
		offset: u64,
		len: u64,
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>>>>;
	fn len(&self) -> u64;
}

pub struct Nrkv<S> {
	hdr: Header,
	store: S,
}

impl<S: Store> Nrkv<S> {
	#[must_use]
	pub async fn init<R>(store: S, random: &mut R, user_data_len: u8) -> Result<Self, S::Error>
	where
		R: RngCore + CryptoRng,
	{
		let mut hash_key = [0; 16];
		random.fill_bytes(&mut hash_key);
		let hdr = || Header {
			hash_key,
			old_nrkv: u64::MAX,
			used: HEADER_SIZE + HAMT_ENTRY_SIZE * HAMT_ROOT_LEN,
			free_head: HEADER_SIZE + HAMT_ENTRY_SIZE * HAMT_ROOT_LEN,
			user_data_len,
		};
		let store = Self { hdr: hdr(), store }.save().await?;
		Ok(Self { hdr: hdr(), store })
	}

	#[must_use]
	pub async fn load(mut store: S) -> Result<Self, S::Error> {
		let mut hdr = [0; _];
		store.read(0, &mut hdr).await?;
		let hdr = Header::from_raw(&hdr);
		Ok(Self { hdr, store })
	}

	pub async fn save(mut self) -> Result<S, S::Error> {
		self.store.write(0, &self.hdr.to_raw()).await?;
		Ok(self.store)
	}

	pub async fn insert(&mut self, key: &Key, data: &[u8]) -> Result<Option<Tag>, S::Error> {
		let h = self.hash(key);
		let next = |h, d| (h / u128::from(d), h % u128::from(d));
		let (mut h, mut i) = next(h, HAMT_ROOT_LEN);
		let Some(mut slot) = self.hamt_root_get(i as _).await? else {
			let offt = self.add_item(key, data).await?;
			self.hamt_root_set(i as _, offt.get()).await?;
			return Ok(Some(offt));
		};

		loop {
			let mut item = Item::new(self, slot);
			if item.key_eq(key).await? {
				return Ok(None);
			}
			(h, i) = next(h, HAMT_CHILD_LEN);
			let Some(s) = item.hamt_get(i as _).await? else {
				let offt = item.kv.add_item(key, data).await?;
				item.hamt_set(i as _, offt.get()).await?;
				return Ok(Some(offt));
			};
			slot = s;
		}
	}

	pub async fn find(&mut self, key: &Key) -> Result<Option<Tag>, S::Error> {
		let h = self.hash(key);
		let next = |h, d| (h / u128::from(d), h % u128::from(d));
		let (mut h, mut i) = next(h, HAMT_ROOT_LEN);
		let mut slot = self.hamt_root_get(i as _).await?;

		while let Some(item) = slot {
			let mut item = Item::new(self, item);
			if item.key_eq(key).await? {
				return Ok(Some(item.offset));
			}
			(h, i) = next(h, HAMT_CHILD_LEN);
			slot = item.hamt_get(i as _).await?;
		}
		Ok(None)
	}

	pub async fn remove(&mut self, key: &Key) -> Result<bool, S::Error> {
		let Some(tag) = self.find(key).await? else { return Ok(false) };
		let offt = tag.get() + u64::from(self.key_offset());
		self.store.write(offt, &[0]).await?;
		Ok(true)
	}

	async fn hamt_root_get(&mut self, index: u16) -> Result<Option<Tag>, S::Error> {
		debug_assert!(u64::from(index) < HAMT_ROOT_LEN);
		let mut buf = [0; 8];
		let offt = 64 + u64::from(index) * HAMT_ENTRY_SIZE;
		self.store.read(offt, &mut buf[..6]).await?;
		Ok(Tag::new(u64::from_le_bytes(buf)))
	}

	async fn hamt_root_set(&mut self, index: u16, value: u64) -> Result<(), S::Error> {
		debug_assert!(u64::from(index) < HAMT_ROOT_LEN);
		let offt = 64 + u64::from(index) * HAMT_ENTRY_SIZE;
		self.store.write(offt, &value.to_le_bytes()[..6]).await
	}

	async fn add_item(&mut self, key: &Key, data: &[u8]) -> Result<Tag, S::Error> {
		assert!(data.len() <= usize::from(self.hdr.user_data_len));
		let offt = self.alloc(self.item_len(key.len() as _).into()).await?;
		self.store.write(offt.get(), data).await?;
		let offt_key = offt.get() + u64::from(self.key_offset());
		self.store.write(offt_key, &[key.len() as _]).await?;
		self.store.write(offt_key + 1, key).await?;
		Ok(offt)
	}

	fn key_offset(&self) -> u16 {
		u16::try_from(HAMT_ENTRY_SIZE * HAMT_CHILD_LEN).unwrap() + u16::from(self.hdr.user_data_len)
	}

	fn item_len(&self, key_len: u8) -> u16 {
		self.key_offset() + 1 + u16::from(key_len)
	}

	fn hash(&self, key: &Key) -> u128 {
		let mut h = SipHasher13::new_with_key(&self.hdr.hash_key);
		h.write(key);
		h.finish128().as_u128()
	}

	pub async fn next_batch(
		&mut self,
		state: &mut IterState,
		f: &mut dyn FnMut(Tag) -> bool,
	) -> Result<(), S::Error> {
		loop {
			if !self.next_batch_child(state, f).await? || !state.step_root() {
				return Ok(());
			}
		}
	}

	async fn next_batch_child(
		&mut self,
		state: &mut IterState,
		f: &mut dyn FnMut(Tag) -> bool,
	) -> Result<bool, S::Error> {
		let Some(root) = self.hamt_root_get(state.root()).await? else { return Ok(true) };

		async fn rec<S: Store>(
			slf: &mut Nrkv<S>,
			item: Tag,
			depth: u8,
			state: &mut IterState,
			f: &mut dyn FnMut(Tag) -> bool,
		) -> Result<bool, S::Error> {
			dbg!(depth, state.child(depth));
			let mut item = Item::new(slf, item);
			if depth == state.depth() {
				state.incr_depth();
				if !f(item.offset) {
					return Ok(false);
				}
			}
			for i in state.child(depth)..=15 {
				if let Some(child) = item.hamt_get(i).await? {
					fn box_fut<'a, T>(
						f: impl Future<Output = T> + 'a,
					) -> Pin<Box<dyn Future<Output = T> + 'a>> {
						Box::pin(f)
					}
					let f = box_fut(rec(item.kv, child, depth + 1, state, f));
					if !f.await? {
						return Ok(false);
					}
				}
				state.step_child(depth);
			}
			state.decr_depth();
			Ok(true)
		}

		rec(self, root, 0, state, f).await
	}

	pub async fn take_all<F, Fut>(
		&mut self,
		old: &mut Self,
		old_id: u64,
		mut f: F,
	) -> Result<(), S::Error>
	where
		for<'f> F: FnMut(&'f mut Self, &[u8], &[u8]) -> Fut + 'f,
		Fut: Future<Output = Result<(), S::Error>>,
	{
		todo!()
	}

	pub async fn read_user_data(
		&mut self,
		tag: Tag,
		offset: u8,
		buf: &mut [u8],
	) -> Result<(), S::Error> {
		assert!(usize::from(offset) + buf.len() <= usize::from(self.hdr.user_data_len));
		let offt = tag.get() + HAMT_CHILD_LEN * HAMT_ENTRY_SIZE + u64::from(offset);
		self.store.read(offt, buf).await
	}

	pub async fn write_user_data(
		&mut self,
		tag: Tag,
		offset: u8,
		data: &[u8],
	) -> Result<(), S::Error> {
		assert!(usize::from(offset) + data.len() <= usize::from(self.hdr.user_data_len));
		let offt = tag.get() + HAMT_CHILD_LEN * HAMT_ENTRY_SIZE + u64::from(offset);
		self.store.write(offt, data).await
	}

	pub async fn alloc(&mut self, amount: u64) -> Result<Tag, S::Error> {
		let offt = Tag::new(self.hdr.free_head).unwrap();
		self.hdr.free_head += u64::from(amount);
		Ok(offt)
	}

	pub async fn dealloc(&mut self, offset: u64, amount: u64) -> Result<(), S::Error> {
		self.hdr.used -= amount;
		self.store.write_zeros(offset, amount).await?;
		Ok(())
	}
}

#[repr(C)]
struct Header {
	hash_key: [u8; 16],
	old_nrkv: u64,
	used: u64,
	free_head: u64,
	user_data_len: u8,
}

impl Header {
	fn to_raw(&self) -> [u8; mem::size_of::<Self>()] {
		fn f<const N: usize>(s: &mut [u8], v: [u8; N]) -> &mut [u8] {
			let (x, y) = s.split_array_mut::<N>();
			*x = v;
			y
		}

		let mut buf = [0; mem::size_of::<Self>()];
		let b = f(&mut buf, self.hash_key);
		let b = f(b, self.old_nrkv.to_le_bytes());
		let b = f(b, self.used.to_le_bytes());
		let b = f(b, self.free_head.to_le_bytes());
		let _ = f(b, self.user_data_len.to_le_bytes());
		buf
	}

	fn from_raw(raw: &[u8; mem::size_of::<Self>()]) -> Self {
		fn f<const N: usize>(s: &mut &[u8]) -> [u8; N] {
			let (x, y) = s.split_array_ref::<N>();
			*s = y;
			*x
		}

		let mut raw = &raw[..];
		Self {
			hash_key: f(&mut raw),
			old_nrkv: u64::from_le_bytes(f(&mut raw)),
			used: u64::from_le_bytes(f(&mut raw)),
			free_head: u64::from_le_bytes(f(&mut raw)),
			user_data_len: u8::from_le_bytes(f(&mut raw)),
		}
	}
}

struct Item<'a, S> {
	kv: &'a mut Nrkv<S>,
	offset: Tag,
}

impl<'a, S> Item<'a, S> {
	fn new(kv: &'a mut Nrkv<S>, offset: Tag) -> Self {
		Self { kv, offset }
	}
}

impl<'a, S: Store> Item<'a, S> {
	async fn key_eq(&mut self, key: &Key) -> Result<bool, S::Error> {
		let offt = self.offset.get() + u64::from(self.kv.key_offset());
		let len = &mut [0];
		self.kv.store.read(offt, len).await?;
		if usize::from(len[0]) != key.len() {
			return Ok(false);
		}
		let mut buf_stack = [0; 32];
		let mut buf_heap = vec![];
		let buf = buf_stack.get_mut(..key.len()).unwrap_or_else(|| {
			buf_heap.resize(key.len(), 0);
			&mut buf_heap[..]
		});
		self.kv.store.read(offt + 1, buf).await?;
		Ok(&buf[..key.len()] == &**key)
	}

	async fn hamt_get(&mut self, index: u8) -> Result<Option<Tag>, S::Error> {
		debug_assert!(index < 16);
		let mut buf = [0; 8];
		let offt = self.offset.get() + u64::from(index) * HAMT_ENTRY_SIZE;
		self.kv.store.read(offt, &mut buf[..6]).await?;
		Ok(Tag::new(u64::from_le_bytes(buf)))
	}

	async fn hamt_set(&mut self, index: u8, value: u64) -> Result<(), S::Error> {
		debug_assert!(index < 16);
		let offt = self.offset.get() + u64::from(index) * HAMT_ENTRY_SIZE;
		self.kv.store.write(offt, &value.to_le_bytes()[..6]).await
	}
}

#[cfg(feature = "alloc")]
impl Store for Vec<u8> {
	type Error = !;

	fn read(
		&mut self,
		offset: u64,
		buf: &mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>>>> {
		let b = offset
			.try_into()
			.ok()
			.and_then(|o| self.get(o..o + buf.len()))
			.expect("out of bounds");
		buf.copy_from_slice(b);
		Box::pin(future::ready(Ok(())))
	}

	fn write(
		&mut self,
		offset: u64,
		data: &[u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>>>> {
		offset
			.try_into()
			.ok()
			.and_then(|o| self.get_mut(o..o + data.len()))
			.expect("out of bounds")
			.copy_from_slice(data);
		Box::pin(future::ready(Ok(())))
	}

	fn write_zeros(
		&mut self,
		offset: u64,
		len: u64,
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>>>> {
		let f = |n: u64| n.try_into().ok();
		f(offset)
			.and_then(|s| offset.checked_add(len).and_then(f).map(|e| s..e))
			.and_then(|r| self.get_mut(r))
			.expect("out of bounds")
			.fill(0);
		Box::pin(future::ready(Ok(())))
	}

	fn len(&self) -> u64 {
		Vec::len(self).try_into().unwrap_or(u64::MAX)
	}
}

use {
	core::{
		fmt,
		num::NonZeroU8,
		ops::{Deref, DerefMut},
	},
	std::{rc::Rc, sync::Arc},
};

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Key([u8]);

#[derive(Debug)]
pub struct TooLong;

impl Key {
	pub(crate) fn len_u8(&self) -> u8 {
		self.len_nonzero_u8().get()
	}

	pub(crate) fn len_nonzero_u8(&self) -> NonZeroU8 {
		debug_assert!(!self.0.is_empty());
		// SAFETY: names are always non-zero length.
		unsafe { NonZeroU8::new_unchecked(self.0.len() as _) }
	}
}

impl<'a> TryFrom<&'a [u8]> for &'a Key {
	type Error = TooLong;

	fn try_from(s: &'a [u8]) -> Result<Self, Self::Error> {
		// SAFETY: Key is repr(transparent)
		(1..=255)
			.contains(&s.len())
			.then(|| unsafe { &*(s as *const _ as *const _) })
			.ok_or(TooLong)
	}
}

impl<'a> TryFrom<&'a mut [u8]> for &'a mut Key {
	type Error = TooLong;

	fn try_from(s: &'a mut [u8]) -> Result<Self, Self::Error> {
		// SAFETY: Key is repr(transparent)
		(1..=255)
			.contains(&s.len())
			.then(|| unsafe { &mut *(s as *mut _ as *mut _) })
			.ok_or(TooLong)
	}
}

impl<'a> TryFrom<&'a str> for &'a Key {
	type Error = TooLong;

	fn try_from(s: &'a str) -> Result<Self, Self::Error> {
		s.as_bytes().try_into()
	}
}

impl TryFrom<Box<[u8]>> for Box<Key> {
	type Error = TooLong;

	fn try_from(s: Box<[u8]>) -> Result<Self, Self::Error> {
		// SAFETY: Key is repr(transparent)
		(1..=255)
			.contains(&s.len())
			.then(|| unsafe { Box::from_raw(Box::into_raw(s) as *mut Key) })
			.ok_or(TooLong)
	}
}

struct KeyLen<const B: usize>;
trait True {}

// TODO CGE pls
macro_rules! from {
	($x:literal, $($y:literal)*) => {
		$(impl True for KeyLen<{$x * 16 + $y}> {})*
	};
	(rept $($x:literal)*) => {
		$(from!($x, 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15);)*
	};
}
from!(0, 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15);
from!(rept 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15);

impl<'a, const N: usize> From<&'a [u8; N]> for &'a Key
where
	KeyLen<N>: True,
{
	fn from(s: &'a [u8; N]) -> Self {
		// SAFETY: Key is repr(transparent)
		unsafe { &*(s.as_slice() as *const _ as *const _) }
	}
}

impl<'a, const N: usize> From<&'a mut [u8; N]> for &'a mut Key
where
	KeyLen<N>: True,
{
	fn from(s: &'a mut [u8; N]) -> Self {
		// SAFETY: Key is repr(transparent)
		unsafe { &mut *(s.as_mut_slice() as *mut _ as *mut _) }
	}
}

impl Deref for Key {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Key {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

macro_rules! alloc {
	($($ty:ident)*) => {
		$(
			impl From<&Key> for $ty<Key> {
				fn from(name: &Key) -> Self {
					// SAFETY: Key is repr(transparent)
					unsafe { $ty::from_raw($ty::into_raw($ty::<[u8]>::from(&name.0)) as *mut _) }
				}
			}
		)*
	};
}

alloc!(Box Rc Arc);

impl ToOwned for Key {
	type Owned = Box<Key>;

	fn to_owned(&self) -> Self::Owned {
		self.into()
	}
}

impl fmt::Debug for Key {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		bstr::BStr::new(&self.0).fmt(f)
	}
}

impl fmt::Display for Key {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		bstr::BStr::new(&self.0).fmt(f)
	}
}

#[cfg(fuzzing)]
impl<'a> arbitrary::Arbitrary<'a> for &'a Key {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let len = u.int_in_range::<usize>(1..=255)?;
		u.bytes(len).map(|b| b.try_into().unwrap())
	}

	fn size_hint(_depth: usize) -> (usize, Option<usize>) {
		(2, Some(256))
	}
}

/// "lossy" iterator, i.e. it may step over some entries in very rare cases.
#[derive(Default)]
pub struct IterState {
	root_depth: u16,
	children: u128,
}

impl IterState {
	pub fn from_u64(n: u64) -> Self {
		Self { root_depth: (n & 0xfff_f).try_into().unwrap(), children: (n >> 16).into() }
	}

	pub fn into_u64(self) -> u64 {
		(self.children as u64) << 16 | u64::from(self.root_depth)
	}

	fn depth(&self) -> u8 {
		u8::try_from(self.root_depth & 0xf).unwrap()
	}

	fn root(&self) -> u16 {
		self.root_depth >> 4
	}

	fn child(&self, depth: u8) -> u8 {
		debug_assert!(depth <= 15);
		((self.children >> 4 * depth) & 0xf).try_into().unwrap()
	}

	fn set_depth(&mut self, depth: u8) {
		debug_assert!(depth <= 15);
		self.root_depth &= !0xf;
		self.root_depth |= u16::from(depth);
		self.children &= u128::MAX >> 4 * (15 - self.depth());
	}

	fn incr_depth(&mut self) {
		self.set_depth(self.depth() + 1);
	}

	fn decr_depth(&mut self) {
		self.set_depth(self.depth() - 1);
	}

	fn step_child(&mut self, depth: u8) -> bool {
		debug_assert!(depth <= 15);
		if u64::from(self.child(depth)) == HAMT_CHILD_LEN - 1 {
			return false;
		}
		self.children += 1 << 4 * depth;
		true
	}

	fn step_root(&mut self) -> bool {
		if u64::from(self.root()) == HAMT_ROOT_LEN - 1 {
			return false;
		}
		self.root_depth += 1 << 4;
		self.children = 0;
		true
	}
}
