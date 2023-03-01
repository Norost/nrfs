use core::fmt;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ObjectId(u64);

impl ObjectId {
	pub const ROOT: Self = Self(0);

	pub(super) fn from_raw(data: &[u8; 7]) -> Self {
		let &[a, b, c, d, e, f, g] = data;
		Self(u64::from_le_bytes([0, a, b, c, d, e, f, g]))
	}

	pub(super) fn to_raw(&self) -> [u8; 7] {
		let [_, raw @ ..] = self.0.to_le_bytes();
		raw
	}
}

impl From<ObjectId> for u64 {
	fn from(id: ObjectId) -> Self {
		id.0 >> 8
	}
}

impl TryFrom<u64> for ObjectId {
	type Error = &'static str;

	fn try_from(id: u64) -> Result<Self, Self::Error> {
		(id <= 0xff_ffff_ffff_ffff)
			.then(|| Self(id << 8))
			.ok_or("id out of range")
	}
}

impl fmt::Debug for ObjectId {
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
			ObjectId::from_raw(&ObjectId(0xdead_42_00).to_raw())
		);
	}

	#[test]
	fn from_to_u64() {
		assert_eq!(0xdead_42, u64::from(ObjectId::try_from(0xdead_42).unwrap()));
	}
}
