use core::fmt;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Index(u32);

impl Index {
	pub const NIN: Self = Self(0);
	pub const MAX: Self = Self(0xff_ffff);

	pub fn from_raw(data: &[u8; 3]) -> Self {
		let &[a, b, c] = data;
		Self(u32::from_le_bytes([0, a, b, c]))
	}

	pub fn to_raw(&self) -> [u8; 3] {
		let [_, a, b, c] = self.0.to_le_bytes();
		[a, b, c]
	}
}

impl From<Index> for u32 {
	fn from(i: Index) -> Self {
		i.0 >> 8
	}
}

impl TryFrom<u32> for Index {
	type Error = &'static str;

	fn try_from(index: u32) -> Result<Self, Self::Error> {
		(index <= Self::MAX.0)
			.then(|| Self(index << 8))
			.ok_or("index out of range")
	}
}

impl From<Index> for u64 {
	fn from(i: Index) -> Self {
		u32::from(i).into()
	}
}

impl fmt::Debug for Index {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		u32::from(*self).fmt(f)
	}
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn from_to_raw() {
		assert_eq!(
			0xdead_42,
			u32::from(Index::from_raw(&Index(0xdead_42_00).to_raw()))
		);
	}

	#[test]
	fn from_to_u32() {
		assert_eq!(0xdead_42, u32::from(Index::try_from(0xdead_42).unwrap()));
	}
}
