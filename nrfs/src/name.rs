use core::{
	fmt,
	ops::{Deref, DerefMut},
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

impl TryFrom<&[u8]> for &Name {
	type Error = TooLong;

	fn try_from(s: &[u8]) -> Result<Self, Self::Error> {
		// SAFETY: Name is repr(transparent)
		(s.len() < 256)
			.then(|| unsafe { &*(s as *const _ as *const _) })
			.ok_or(TooLong)
	}
}

impl TryFrom<&str> for &Name {
	type Error = TooLong;

	fn try_from(s: &str) -> Result<Self, Self::Error> {
		s.as_bytes().try_into()
	}
}

// CGE pls
macro_rules! from {
	{ $($n:literal)* } => {
		$(
			impl From<&[u8; $n]> for &Name {
				fn from(s: &[u8; $n]) -> Self {
					// SAFETY: Name is repr(transparent)
					unsafe { &*(<&[u8]>::from(s) as *const _ as *const _) }
				}
			}

			impl From<&mut [u8; $n]> for &mut Name {
				fn from(s: &mut [u8; $n]) -> Self {
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

impl fmt::Debug for Name {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		// TODO use Utf8Lossy when it becomes stable.
		String::from_utf8_lossy(self).fmt(f)
	}
}
