#[cfg(feature = "alloc")]
use alloc::{borrow::ToOwned, boxed::Box, rc::Rc, sync::Arc};
use core::{
	fmt,
	num::NonZeroU8,
	ops::{Deref, DerefMut},
};

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

#[cfg(feature = "alloc")]
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

#[cfg(feature = "alloc")]
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

#[cfg(feature = "alloc")]
alloc!(Box Rc Arc);

#[cfg(feature = "alloc")]
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
