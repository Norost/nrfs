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
pub struct Name([u8]);

#[derive(Debug)]
pub struct TooLong;

impl Name {
	pub(crate) fn len_u8(&self) -> u8 {
		self.len_nonzero_u8().get()
	}

	pub(crate) fn len_nonzero_u8(&self) -> NonZeroU8 {
		debug_assert!(!self.0.is_empty());
		// SAFETY: names are always non-zero length.
		unsafe { NonZeroU8::new_unchecked(self.0.len() as _) }
	}
}

impl<'a> TryFrom<&'a [u8]> for &'a Name {
	type Error = TooLong;

	fn try_from(s: &'a [u8]) -> Result<Self, Self::Error> {
		// SAFETY: Name is repr(transparent)
		(1..=255)
			.contains(&s.len())
			.then(|| unsafe { &*(s as *const _ as *const _) })
			.ok_or(TooLong)
	}
}

impl<'a> TryFrom<&'a mut [u8]> for &'a mut Name {
	type Error = TooLong;

	fn try_from(s: &'a mut [u8]) -> Result<Self, Self::Error> {
		// SAFETY: Name is repr(transparent)
		(1..=255)
			.contains(&s.len())
			.then(|| unsafe { &mut *(s as *mut _ as *mut _) })
			.ok_or(TooLong)
	}
}

impl<'a> TryFrom<&'a str> for &'a Name {
	type Error = TooLong;

	fn try_from(s: &'a str) -> Result<Self, Self::Error> {
		s.as_bytes().try_into()
	}
}

impl TryFrom<Box<[u8]>> for Box<Name> {
	type Error = TooLong;

	fn try_from(s: Box<[u8]>) -> Result<Self, Self::Error> {
		// SAFETY: Name is repr(transparent)
		(1..=255)
			.contains(&s.len())
			.then(|| unsafe { Box::from_raw(Box::into_raw(s) as *mut Name) })
			.ok_or(TooLong)
	}
}

// CGE pls
macro_rules! from {
	{ $($n:literal)* } => {
		$(
			impl<'a> From<&'a [u8; $n]> for &'a Name {
				fn from(s: &'a [u8; $n]) -> Self {
					// SAFETY: Name is repr(transparent)
					unsafe { &*(s.as_slice() as *const _ as *const _) }
				}
			}

			impl<'a> From<&'a mut [u8; $n]> for &'a mut Name {
				fn from(s: &'a mut [u8; $n]) -> Self {
					// SAFETY: Name is repr(transparent)
					unsafe { &mut *(s.as_mut_slice() as *mut _ as *mut _) }
				}
			}
		)*
	};
}

from! { 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 }

impl Deref for Name {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Name {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

macro_rules! alloc {
	($($ty:ident)*) => {
		$(
			impl From<&Name> for $ty<Name> {
				fn from(name: &Name) -> Self {
					// SAFETY: Name is repr(transparent)
					unsafe { $ty::from_raw($ty::into_raw($ty::<[u8]>::from(&name.0)) as *mut _) }
				}
			}
		)*
	};
}

alloc!(Box Rc Arc);

impl ToOwned for Name {
	type Owned = Box<Name>;

	fn to_owned(&self) -> Self::Owned {
		self.into()
	}
}

impl fmt::Debug for Name {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		#[cfg(not(fuzzing))]
		{
			bstr::BStr::new(&self.0).fmt(f)
		}
		#[cfg(fuzzing)]
		{
			// Cheat a little to make our lives easier.
			format_args!("b{:?}.into()", &bstr::BStr::new(&self.0)).fmt(f)
		}
	}
}

impl fmt::Display for Name {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		bstr::BStr::new(&self.0).fmt(f)
	}
}

#[cfg(any(test, fuzzing))]
impl<'a> arbitrary::Arbitrary<'a> for &'a Name {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let len = u.int_in_range::<usize>(1..=255)?;
		u.bytes(len).map(|b| b.try_into().unwrap())
	}

	fn size_hint(_depth: usize) -> (usize, Option<usize>) {
		(2, Some(256))
	}
}
