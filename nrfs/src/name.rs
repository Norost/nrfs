use {
	core::{
		fmt,
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
		self.0.len() as _
	}
}

impl Default for &Name {
	fn default() -> Self {
		(&[]).into()
	}
}

impl Default for &mut Name {
	fn default() -> Self {
		(&mut []).into()
	}
}

impl<'a> TryFrom<&'a [u8]> for &'a Name {
	type Error = TooLong;

	fn try_from(s: &'a [u8]) -> Result<Self, Self::Error> {
		// SAFETY: Name is repr(transparent)
		(s.len() < 256)
			.then(|| unsafe { &*(s as *const _ as *const _) })
			.ok_or(TooLong)
	}
}

impl<'a> TryFrom<&'a str> for &'a Name {
	type Error = TooLong;

	fn try_from(s: &'a str) -> Result<Self, Self::Error> {
		s.as_bytes().try_into()
	}
}

// CGE pls
macro_rules! from {
	{ $($n:literal)* } => {
		$(
			impl<'a> From<&'a [u8; $n]> for &'a Name {
				fn from(s: &'a [u8; $n]) -> Self {
					// SAFETY: Name is repr(transparent)
					unsafe { &*(<&[u8]>::from(s) as *const _ as *const _) }
				}
			}

			impl<'a> From<&'a mut [u8; $n]> for &'a mut Name {
				fn from(s: &'a mut [u8; $n]) -> Self {
					// SAFETY: Name is repr(transparent)
					unsafe { &mut *(<&mut [u8]>::from(s) as *mut _ as *mut _) }
				}
			}
		)*
	};
}

from! { 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 }

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

impl fmt::Debug for Name {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		// TODO use Utf8Lossy when it becomes stable.
		String::from_utf8_lossy(self).fmt(f)
	}
}

impl fmt::Display for Name {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		// TODO use Utf8Lossy when it becomes stable.
		String::from_utf8_lossy(self).fmt(f)
	}
}
