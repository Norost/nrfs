#![cfg_attr(not(test), no_std)]
#![deny(unused_must_use)]
#![feature(const_waker, generic_arg_infer, never_type, split_array)]

#[cfg(feature = "alloc")]
extern crate alloc;

mod conf;
mod header;
mod key;
mod store;
#[cfg(test)]
mod test;

pub use {conf::*, key::*, store::*};

use {
	alloc::boxed::Box,
	core::{
		cell::{RefCell, RefMut},
		fmt,
		future::Future,
		hash::Hasher,
		pin::Pin,
	},
	header::*,
	rand_core::{CryptoRng, RngCore},
	siphasher::sip128::{Hasher128, SipHasher13},
};

const HEADER_SIZE: u64 = 64;
const HAMT_ENTRY_SIZE: u64 = 6;
const HAMT_ROOT_LEN: u64 = 4096;
const HAMT_CHILD_LEN: u64 = 16;
const HASH_KEY_OFFSET: u64 = 0;

pub type Tag = core::num::NonZeroU64;

pub struct Nrkv<S, C> {
	store: S,
	conf: C,
}

impl<S, C> Nrkv<S, C> {
	pub fn inner(&self) -> (&S, &C) {
		(&self.store, &self.conf)
	}

	pub fn inner_mut(&mut self) -> (&mut S, &mut C) {
		(&mut self.store, &mut self.conf)
	}

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
		let hdr = Header::new(hash_key, conf.header_offset());
		let mut slf = Self { store, conf };
		slf.set_header(&hdr).await?;
		Ok(slf)
	}

	#[must_use]
	pub fn wrap(store: S, conf: C) -> Self {
		Self { store, conf }
	}

	async fn header(&mut self) -> Result<Header, S::Error> {
		let hdr = &mut [0; HEADER_SIZE as _];
		self.read(self.conf.header_offset(), hdr).await?;
		Ok(Header::from_raw(hdr))
	}

	async fn set_header(&mut self, header: &Header) -> Result<(), S::Error> {
		self.write(self.conf.header_offset(), &header.to_raw())
			.await
	}

	pub async fn insert(&mut self, key: &Key, data: &[u8]) -> Result<Result<Tag, Tag>, S::Error> {
		let h = self.hash(key).await?;
		let next = |h, d| (h / u128::from(d), h % u128::from(d));
		let (mut h, mut i) = next(h, HAMT_ROOT_LEN);
		let (mut slot_offt, slot) = self.hamt_root_get(i as _).await?;
		let Some(mut slot) = slot else {
			let offt = self.replace_item(None, key, data).await?;
			self.hamt_set_entry(slot_offt, offt.get()).await?;
			return Ok(Ok(offt));
		};

		let mut replace = None;
		loop {
			let mut item = Item::new(self, slot);
			match item.key_eq(key).await? {
				None => {
					replace.get_or_insert((slot_offt, slot));
				}
				Some(false) => {}
				Some(true) => return Ok(Err(slot)),
			}
			(h, i) = next(h, HAMT_CHILD_LEN);
			let (o, s) = item.hamt_get(i as _).await?;
			let Some(s) = s else {
				let (o, prev_slot) = replace.map_or((o, None), |(o, s)| (o, Some(s)));
				let offt = item.kv.replace_item(prev_slot, key, data).await?;
				self.hamt_set_entry(o, offt.get()).await?;
				return Ok(Ok(offt));
			};
			(slot_offt, slot) = (o, s);
		}
	}

	pub async fn find(&mut self, key: &Key) -> Result<Option<Tag>, S::Error> {
		let h = self.hash(key).await?;
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

		if let Some(prev_slot) = prev_slot {
			let b = &mut [0; 8];
			self.read(prev_slot.get() - 8, b).await?;
			let b = u64::from_le_bytes(*b);
			assert!(b & 1 == 1, "not allocated");
			self.dealloc(prev_slot.get(), (b >> 16) - 16).await?;
		}

		Ok(offt)
	}

	fn item_len(&self, key_len: u8) -> u64 {
		u64::from(self.conf.item_offset())
			+ (HAMT_CHILD_LEN * HAMT_ENTRY_SIZE)
			+ (1 + u64::from(key_len))
	}

	async fn hash(&mut self, data: &[u8]) -> Result<u128, S::Error> {
		let h = self.hash_key().await?;
		let mut h = SipHasher13::new_with_key(&h);
		h.write(data);
		Ok(h.finish128().as_u128())
	}

	async fn hash_key(&mut self) -> Result<[u8; 16], S::Error> {
		let b = &mut [0; 16];
		self.read(self.conf.header_offset() + HASH_KEY_OFFSET, b)
			.await?;
		Ok(*b)
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
		assert!(usize::from(offset) + <[u8]>::len(buf) <= usize::from(self.conf.item_offset()));
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

	pub async fn alloc(&mut self, len: u64) -> Result<Tag, S::Error> {
		if len == 0 {
			return Ok(Tag::new(16).unwrap());
		}
		let len = (len + 15) & !15;
		let len = 8 + len + 8;
		assert!(len < 1 << 48);

		let hdr = &mut self.header().await?;
		let (offt, prev_region_len) = hdr.alloc(len).unwrap();
		self.set_header(hdr).await?;

		let marker = &(len << 16 | 1).to_le_bytes();
		self.write(offt.get(), marker).await?;
		self.write(offt.get() + len - 8, marker).await?;

		if prev_region_len > 0 && prev_region_len - len > 0 {
			let marker = &(prev_region_len - len << 16 | 0).to_le_bytes();
			self.write(offt.get() + len, marker).await?;
			self.write(offt.get() + prev_region_len - 8, marker).await?;
		}
		Ok(offt.checked_add(8).unwrap())
	}

	pub async fn dealloc(&mut self, offset: u64, len: u64) -> Result<(), S::Error> {
		if len == 0 {
			return Ok(());
		}
		let len = (len + 15) & !15;
		let len = 8 + len + 8;
		let mut start @ mut zero_start = offset - 8;
		let mut end @ mut zero_end = start + len;

		let hdr = &mut self.header().await?;
		hdr.dealloc(end - start).unwrap();
		self.set_header(hdr).await?;

		let f = |b: [u8; 8]| {
			let b = u64::from_le_bytes(b);
			(b >> 16, b as u16)
		};

		let mut b = [0; 8];
		self.read(start - 8, &mut b).await?;
		let (l, flags) = f(b);
		if flags & 1 == 0 {
			start -= l;
			zero_start -= 8 * u64::from(l > 0);
		}

		self.read(end, &mut b).await?;
		let (l, flags) = f(b);
		if flags & 1 == 0 {
			end += l;
			zero_end += 8 * u64::from(l > 0);
		}

		self.write_zeros(zero_start, zero_end - zero_start).await?;

		let l = end - start;
		if hdr.insert_free_region(start, l) {
			self.write_zeros(start, 8).await?;
		} else {
			let marker = &(l << 16 | 0).to_le_bytes();
			self.write(start, marker).await?;
			self.write(end - 8, marker).await?;
		}
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

impl<S, C: fmt::Debug> fmt::Debug for Nrkv<S, C> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Nrkv))
			.field("conf", &self.conf)
			.finish_non_exhaustive()
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

		// FIXME make non-recursive to avoid alloc
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
		let buf = &mut [0; 255][..key.len()];
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
		let l = <[u8]>::len(buf).min(usize::from(len[0]));
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

fn u64_to_u48(n: u64) -> Option<[u8; 6]> {
	(n < 1 << 48).then(|| n.to_le_bytes()[..6].try_into().unwrap())
}

fn u48_to_u64(n: [u8; 6]) -> u64 {
	let mut b = [0; 8];
	b[..6].copy_from_slice(&n);
	u64::from_le_bytes(b)
}

fn apply_u48(n: [u8; 6], f: impl FnOnce(u64) -> u64) -> Option<[u8; 6]> {
	u64_to_u48(f(u48_to_u64(n)))
}
