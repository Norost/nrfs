//#![no_std]
#![deny(unused_must_use)]
#![feature(const_waker, generic_arg_infer, never_type, split_array)]

#[cfg(test)]
mod test;

extern crate alloc;

use {
	alloc::boxed::Box,
	core::{
		cell::{RefCell, RefMut},
		fmt,
		future::{self, Future},
		hash::Hasher,
		num::NonZeroU8,
		ops::{Deref, DerefMut},
		pin::Pin,
	},
	rand_core::{CryptoRng, RngCore},
	siphasher::sip128::{Hasher128, SipHasher13},
	std::{rc::Rc, sync::Arc},
};

const HEADER_SIZE: u64 = 28;
const HAMT_ENTRY_SIZE: u64 = 6;
const HAMT_ROOT_LEN: u64 = 4096;
const HAMT_CHILD_LEN: u64 = 16;

pub type Tag = core::num::NonZeroU64;

pub trait Store {
	type Error;

	fn read<'a>(
		&'a mut self,
		offset: u64,
		buf: &'a mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
	fn write<'a>(
		&'a mut self,
		offset: u64,
		data: &'a [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
	fn write_zeros<'a>(
		&'a mut self,
		offset: u64,
		len: u64,
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
	fn len(&self) -> u64;
}

pub trait Conf {
	fn header_offset(&self) -> u64;
	fn item_offset(&self) -> u16;
}

pub struct Nrkv<S, C> {
	hdr: Header,
	store: S,
	conf: C,
}

impl<S, C> Nrkv<S, C> {
	pub fn into_inner(self) -> (S, C) {
		(self.store, self.conf)
	}
}

impl<S: Store, C: Conf> Nrkv<S, C> {
	#[must_use]
	pub async fn init<R>(store: S, conf: C, random: &mut R) -> Result<Self, S::Error>
	where
		R: RngCore + CryptoRng,
	{
		let mut hash_key = [0; 16];
		random.fill_bytes(&mut hash_key);
		Self::init_with_key(store, conf, hash_key).await
	}

	#[must_use]
	pub async fn init_with_key(store: S, conf: C, hash_key: [u8; 16]) -> Result<Self, S::Error> {
		let used = conf.header_offset() + HEADER_SIZE + HAMT_ENTRY_SIZE * HAMT_ROOT_LEN;
		let hdr = Header { hash_key, used: u64_to_u48(used), free_head: u64_to_u48(used) };
		let mut slf = Self { hdr, store, conf };
		slf.save().await?;
		Ok(slf)
	}

	#[must_use]
	pub async fn load(mut store: S, conf: C) -> Result<Self, S::Error> {
		let mut hdr = [0; _];
		store.read(conf.header_offset(), &mut hdr).await?;
		let hdr = Header::from_raw(&hdr);
		Ok(Self { hdr, store, conf })
	}

	pub async fn save(&mut self) -> Result<(), S::Error> {
		self.write(self.conf.header_offset(), &self.hdr.to_raw())
			.await
	}

	pub async fn insert(&mut self, key: &Key, data: &[u8]) -> Result<Option<Tag>, S::Error> {
		let h = self.hash(key);
		let next = |h, d| (h / u128::from(d), h % u128::from(d));
		let (mut h, mut i) = next(h, HAMT_ROOT_LEN);
		let (mut slot_offt, slot) = self.hamt_root_get(i as _).await?;
		let Some(mut slot) = slot else {
			let offt = self.replace_item(None, key, data).await?;
			self.hamt_set_entry(slot_offt, offt.get()).await?;
			return Ok(Some(offt));
		};

		let mut replace = None;
		loop {
			let mut item = Item::new(self, slot);
			match item.key_eq(key).await? {
				None => {
					replace.get_or_insert((slot_offt, slot));
				}
				Some(false) => {}
				Some(true) => return Ok(None),
			}
			(h, i) = next(h, HAMT_CHILD_LEN);
			let (o, s) = item.hamt_get(i as _).await?;
			let Some(s) = s else {
				let (o, prev_slot) = replace.map_or((o, None), |(o, s)| (o, Some(s)));
				let offt = item.kv.replace_item(prev_slot, key, data).await?;
				self.hamt_set_entry(o, offt.get()).await?;
				return Ok(Some(offt));
			};
			(slot_offt, slot) = (o, s);
		}
	}

	pub async fn find(&mut self, key: &Key) -> Result<Option<Tag>, S::Error> {
		let h = self.hash(key);
		let next = |h, d| (h / u128::from(d), h % u128::from(d));
		let (mut h, mut i) = next(h, HAMT_ROOT_LEN);
		let (_, mut slot) = self.hamt_root_get(i as _).await?;

		while let Some(item) = slot {
			let mut item = Item::new(self, item);
			if item.key_eq(key).await? == Some(true) {
				return Ok(Some(item.offset));
			}
			(h, i) = next(h, HAMT_CHILD_LEN);
			(_, slot) = item.hamt_get(i as _).await?;
		}
		Ok(None)
	}

	pub async fn remove(&mut self, tag: Tag) -> Result<(), S::Error> {
		Item::new(self, tag).erase_key().await
	}

	async fn hamt_root_get(&mut self, index: u16) -> Result<(Tag, Option<Tag>), S::Error> {
		debug_assert!(u64::from(index) < HAMT_ROOT_LEN);
		let mut buf = [0; 8];
		let offt = self.conf.header_offset() + HEADER_SIZE + u64::from(index) * HAMT_ENTRY_SIZE;
		let offt = Tag::new(offt).unwrap();
		self.read(offt.get(), &mut buf[..6]).await?;
		Ok((offt, Tag::new(u64::from_le_bytes(buf))))
	}

	async fn hamt_set_entry(&mut self, offset: Tag, value: u64) -> Result<(), S::Error> {
		assert!(value < 1 << 48);
		self.write(offset.get(), &value.to_le_bytes()[..6]).await
	}

	async fn replace_item(
		&mut self,
		prev_slot: Option<Tag>,
		key: &Key,
		data: &[u8],
	) -> Result<Tag, S::Error> {
		assert!(data.len() <= usize::from(self.conf.item_offset()));
		let offt = self.alloc(self.item_len(key.len_u8())).await?;

		Item::new(self, offt).write_user(0, data).await?;

		if let Some(prev_slot) = prev_slot {
			let mut buf = [0; _];
			Item::new(self, prev_slot).read_hamt(&mut buf).await?;
			Item::new(self, offt).write_hamt(&buf).await?;
		}

		Item::new(self, offt).write_key(key).await?;

		Ok(offt)
	}

	fn item_len(&self, key_len: u8) -> u64 {
		u64::from(self.conf.item_offset())
			+ (HAMT_CHILD_LEN * HAMT_ENTRY_SIZE)
			+ (1 + u64::from(key_len))
	}

	pub fn hash(&self, data: &[u8]) -> u128 {
		let mut h = SipHasher13::new_with_key(&self.hdr.hash_key);
		h.write(data);
		h.finish128().as_u128()
	}

	pub fn hash_key(&self) -> [u8; 16] {
		self.hdr.hash_key
	}

	pub async fn read_key(&mut self, tag: Tag, buf: &mut [u8]) -> Result<u8, S::Error> {
		Item::new(self, tag).read_key(buf).await
	}

	pub async fn read_user_data(
		&mut self,
		tag: Tag,
		offset: u16,
		buf: &mut [u8],
	) -> Result<(), S::Error> {
		assert!(usize::from(offset) + buf.len() <= usize::from(self.conf.item_offset()));
		Item::new(self, tag).read_user(offset, buf).await
	}

	pub async fn write_user_data(
		&mut self,
		tag: Tag,
		offset: u16,
		data: &[u8],
	) -> Result<(), S::Error> {
		assert!(usize::from(offset) + data.len() <= usize::from(self.conf.item_offset()));
		Item::new(self, tag).write_user(offset, data).await
	}

	pub async fn alloc(&mut self, amount: u64) -> Result<Tag, S::Error> {
		let offt = Tag::new(u48_to_u64(self.hdr.free_head)).unwrap();
		apply_u48(&mut self.hdr.free_head, |n| n + u64::from(amount));
		Ok(offt)
	}

	pub async fn dealloc(&mut self, offset: u64, amount: u64) -> Result<(), S::Error> {
		apply_u48(&mut self.hdr.used, |n| n - amount);
		self.write_zeros(offset, amount).await?;
		Ok(())
	}

	pub async fn read(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), S::Error> {
		self.store.read(offset, buf).await
	}

	pub async fn write(&mut self, offset: u64, data: &[u8]) -> Result<(), S::Error> {
		self.store.write(offset, data).await
	}

	pub async fn write_zeros(&mut self, offset: u64, len: u64) -> Result<(), S::Error> {
		self.store.write_zeros(offset, len).await
	}
}

pub struct ShareNrkv<'a, S, C>(RefCell<&'a mut Nrkv<S, C>>);

impl<'a, S, C> ShareNrkv<'a, S, C> {
	pub fn new(kv: &'a mut Nrkv<S, C>) -> Self {
		Self(RefCell::new(kv))
	}

	pub fn borrow_mut(&self) -> RefMut<Nrkv<S, C>> {
		RefMut::map(self.0.borrow_mut(), |b| *b)
	}
}

impl<'a, S: Store, C: Conf> ShareNrkv<'a, S, C> {
	pub async fn next_batch<F, Fut>(&self, state: &mut IterState, mut f: F) -> Result<(), S::Error>
	where
		F: FnMut(Tag) -> Fut,
		Fut: Future<Output = Result<bool, S::Error>>,
	{
		while self.next_batch_child(state, &mut f).await? {
			if !state.step_root() {
				state.set_depth(15);
				break;
			}
		}
		Ok(())
	}

	async fn next_batch_child<F, Fut>(
		&self,
		state: &mut IterState,
		f: &mut F,
	) -> Result<bool, S::Error>
	where
		F: FnMut(Tag) -> Fut,
		Fut: Future<Output = Result<bool, S::Error>>,
	{
		let (_, Some(root)) = self.borrow_mut().hamt_root_get(state.root()).await?
			else { return Ok(true) };

		async fn rec<S: Store, C: Conf, F, Fut>(
			slf: &ShareNrkv<'_, S, C>,
			item: Tag,
			depth: u8,
			state: &mut IterState,
			f: &mut F,
		) -> Result<bool, S::Error>
		where
			F: FnMut(Tag) -> Fut,
			Fut: Future<Output = Result<bool, S::Error>>,
		{
			if depth == state.depth() {
				state.incr_depth();
				if !f(item).await? {
					return Ok(false);
				}
			}
			for i in state.child(depth)..=15 {
				let mut kv = slf.borrow_mut();
				let mut item = Item::new(&mut kv, item);
				if let (_, Some(child)) = item.hamt_get(i).await? {
					fn box_fut<'a, T>(
						f: impl Future<Output = T> + 'a,
					) -> Pin<Box<dyn Future<Output = T> + 'a>> {
						Box::pin(f)
					}
					drop(kv);
					let f = box_fut(rec(slf, child, depth + 1, state, f));
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
}

#[repr(C)]
struct Header {
	hash_key: [u8; 16],
	used: [u8; 6],
	free_head: [u8; 6],
}

impl Header {
	fn to_raw(&self) -> [u8; HEADER_SIZE as _] {
		fn f<const N: usize>(s: &mut [u8], v: [u8; N]) -> &mut [u8] {
			let (x, y) = s.split_array_mut::<N>();
			*x = v;
			y
		}

		let mut buf = [0; _];
		let b = f(&mut buf, self.hash_key);
		let b = f(b, self.used);
		let b = f(b, self.free_head);
		assert!(b.is_empty());
		buf
	}

	fn from_raw(raw: &[u8; HEADER_SIZE as _]) -> Self {
		fn f<const N: usize>(s: &mut &[u8]) -> [u8; N] {
			let (x, y) = s.split_array_ref::<N>();
			*s = y;
			*x
		}

		let mut raw = &raw[..];
		let s = Self { hash_key: f(&mut raw), used: f(&mut raw), free_head: f(&mut raw) };
		assert!(raw.is_empty());
		s
	}
}

struct Item<'a, S, C> {
	kv: &'a mut Nrkv<S, C>,
	offset: Tag,
}

impl<'a, S, C> Item<'a, S, C> {
	fn new(kv: &'a mut Nrkv<S, C>, offset: Tag) -> Self {
		Self { kv, offset }
	}
}

impl<'a, S: Store, C: Conf> Item<'a, S, C> {
	async fn key_eq(&mut self, key: &Key) -> Result<Option<bool>, S::Error> {
		let len = &mut [0];
		self.read(self.key_offset(), len).await?;
		if len[0] == 0 {
			return Ok(None);
		} else if usize::from(len[0]) != key.len() {
			return Ok(Some(false));
		}
		let mut buf_stack = [0; 32];
		let mut buf_heap = vec![];
		let buf = buf_stack.get_mut(..key.len()).unwrap_or_else(|| {
			buf_heap.resize(key.len(), 0);
			&mut buf_heap[..]
		});
		self.read(self.key_offset() + 1, buf).await?;
		Ok(Some(&buf[..key.len()] == &**key))
	}

	async fn hamt_get(&mut self, index: u8) -> Result<(Tag, Option<Tag>), S::Error> {
		debug_assert!(u64::from(index) < HAMT_CHILD_LEN);
		let offt = self.offset.get() + self.hamt_offset() + u64::from(index) * HAMT_ENTRY_SIZE;
		let offt = Tag::new(offt).unwrap();
		let mut buf = [0; 8];
		self.kv.read(offt.get(), &mut buf[..6]).await?;
		Ok((offt, Tag::new(u64::from_le_bytes(buf))))
	}

	fn hamt_offset(&self) -> u64 {
		u64::from(self.kv.conf.item_offset())
	}

	fn key_offset(&self) -> u64 {
		self.hamt_offset() + HAMT_CHILD_LEN * HAMT_ENTRY_SIZE
	}

	async fn read(&mut self, offt: u64, buf: &mut [u8]) -> Result<(), S::Error> {
		self.kv.read(self.offset.get() + offt, buf).await
	}

	async fn write(&mut self, offt: u64, data: &[u8]) -> Result<(), S::Error> {
		self.kv.write(self.offset.get() + offt, data).await
	}

	async fn read_user(&mut self, offset: u16, buf: &mut [u8]) -> Result<(), S::Error> {
		self.read(offset.into(), buf).await
	}

	async fn write_user(&mut self, offset: u16, data: &[u8]) -> Result<(), S::Error> {
		self.write(offset.into(), data).await
	}

	async fn read_hamt(
		&mut self,
		buf: &mut [u8; (HAMT_CHILD_LEN * HAMT_ENTRY_SIZE) as _],
	) -> Result<(), S::Error> {
		self.read(self.hamt_offset(), buf).await
	}

	async fn write_hamt(
		&mut self,
		data: &[u8; (HAMT_CHILD_LEN * HAMT_ENTRY_SIZE) as _],
	) -> Result<(), S::Error> {
		self.write(self.hamt_offset(), data).await
	}

	async fn read_key(&mut self, buf: &mut [u8]) -> Result<u8, S::Error> {
		let len = &mut [0];
		self.read(self.key_offset(), len).await?;
		let l = buf.len().min(usize::from(len[0]));
		self.read(self.key_offset() + 1, &mut buf[..l]).await?;
		Ok(len[0])
	}

	async fn write_key(&mut self, key: &Key) -> Result<(), S::Error> {
		self.write(self.key_offset(), &[key.len_u8()]).await?;
		self.write(self.key_offset() + 1, key).await
	}

	async fn erase_key(&mut self) -> Result<(), S::Error> {
		self.write(self.key_offset(), &[0]).await
	}
}

impl Store for [u8] {
	type Error = !;

	fn read(
		&mut self,
		offset: u64,
		buf: &mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + '_>> {
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
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + '_>> {
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
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + '_>> {
		let f = |n: u64| n.try_into().ok();
		f(offset)
			.and_then(|s| offset.checked_add(len).and_then(f).map(|e| s..e))
			.and_then(|r| self.get_mut(r))
			.expect("out of bounds")
			.fill(0);
		Box::pin(future::ready(Ok(())))
	}

	fn len(&self) -> u64 {
		<[u8]>::len(self).try_into().unwrap_or(u64::MAX)
	}
}

macro_rules! store_slice {
	($ty:ty) => {
		impl Store for $ty {
			type Error = !;

			fn read<'a>(
				&'a mut self,
				offset: u64,
				buf: &'a mut [u8],
			) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>> {
				<[u8] as Store>::read(self, offset, buf)
			}
			fn write<'a>(
				&'a mut self,
				offset: u64,
				data: &'a [u8],
			) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>> {
				<[u8] as Store>::write(self, offset, data)
			}
			fn write_zeros<'a>(
				&'a mut self,
				offset: u64,
				len: u64,
			) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>> {
				<[u8] as Store>::write_zeros(self, offset, len)
			}
			fn len(&self) -> u64 {
				<[u8] as Store>::len(self)
			}
		}
	};
}
#[cfg(feature = "alloc")]
store_slice!(Box<[u8]>);
#[cfg(feature = "alloc")]
store_slice!(Vec<u8>);

#[derive(Debug)]
pub struct DynConf {
	pub header_offset: u64,
	pub item_offset: u16,
}

impl Conf for DynConf {
	fn header_offset(&self) -> u64 {
		self.header_offset
	}
	fn item_offset(&self) -> u16 {
		self.item_offset
	}
}

#[derive(Debug)]
pub struct StaticConf<const HEADER_OFFSET: u64, const ITEM_OFFSET: u16>;

impl<const HEADER_OFFSET: u64, const ITEM_OFFSET: u16> StaticConf<HEADER_OFFSET, ITEM_OFFSET> {
	pub const CONF: Self = Self;
}

impl<const HEADER_OFFSET: u64, const ITEM_OFFSET: u16> Conf
	for StaticConf<HEADER_OFFSET, ITEM_OFFSET>
{
	fn header_offset(&self) -> u64 {
		HEADER_OFFSET
	}
	fn item_offset(&self) -> u16 {
		ITEM_OFFSET
	}
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Key([u8]);

#[derive(Debug)]
pub struct TooLong;

impl Key {
	pub fn len_u8(&self) -> u8 {
		self.len_nonzero_u8().get()
	}

	pub fn len_nonzero_u8(&self) -> NonZeroU8 {
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
		self.children &= !(u128::MAX << 4 * self.depth());
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

impl fmt::Debug for IterState {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(IterState))
			.field("depth", &self.depth())
			.field("root", &format_args!("{:03x}", self.root()))
			.field("children", &format_args!("{:016x}", self.children))
			.finish()
	}
}

fn u64_to_u48(n: u64) -> [u8; 6] {
	n.to_le_bytes()[..6].try_into().unwrap()
}

fn u48_to_u64(n: [u8; 6]) -> u64 {
	let mut b = [0; 8];
	b[..6].copy_from_slice(&n);
	u64::from_le_bytes(b)
}

fn apply_u48(n: &mut [u8; 6], f: impl FnOnce(u64) -> u64) {
	*n = u64_to_u48(f(u48_to_u64(*n)));
}
