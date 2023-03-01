use core::fmt;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Offset(u64);

impl Offset {
	pub const MIN: Self = Self(0);
	pub const MAX: Self = Self(0xffff_ffff_ffff);

	pub fn from_raw(data: &[u8; 6]) -> Self {
		let &[a, b, c, d, e, f] = data;
		Self(u64::from_le_bytes([0, 0, a, b, c, d, e, f]))
	}

	pub fn to_raw(&self) -> [u8; 6] {
		let [_, _, a, b, c, d, e, f] = self.0.to_le_bytes();
		[a, b, c, d, e, f]
	}

	pub fn add_u64(&self, n: u64) -> Option<Self> {
		u64::from(*self)
			.checked_add(n)
			.and_then(|x| Self::try_from(x).ok())
	}
}

impl From<Offset> for u64 {
	fn from(offt: Offset) -> Self {
		offt.0 >> 16
	}
}

impl TryFrom<u64> for Offset {
	type Error = &'static str;

	fn try_from(offset: u64) -> Result<Self, Self::Error> {
		(offset <= Self::MAX.0)
			.then(|| Self(offset << 16))
			.ok_or("offset out of range")
	}
}

impl fmt::Debug for Offset {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		u64::from(*self).fmt(f)
	}
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn from_to_raw() {
		assert_eq!(
			0xdead_42,
			Offset::from_raw(&Offset(0xdead_42_0000).to_raw())
		);
	}

	#[test]
	fn from_to_u64() {
		assert_eq!(0xdead_42, u64::from(Offset::try_from(0xdead_42).unwrap()));
	}
}
