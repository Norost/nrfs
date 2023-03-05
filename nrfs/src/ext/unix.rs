#[derive(Clone, Copy, Debug, Default)]
pub struct Unix {
	pub permissions: u16,
	uid: [u8; 3],
	gid: [u8; 3],
}

impl Unix {
	/// Construct new [`Unix`].
	///
	/// # Panics
	///
	/// If `uid` or `gid` do not fit in 24 bits.
	pub fn new(permissions: u16, uid: u32, gid: u32) -> Self {
		let mut s = Self { permissions, uid: [0; 3], gid: [0; 3] };
		s.set_uid(uid);
		s.set_gid(gid);
		s
	}

	pub(crate) fn from_raw(data: [u8; 8]) -> Self {
		let [a, b, c, d, e, f, g, h] = data;
		Self { permissions: u16::from_le_bytes([a, b]) >> 1, uid: [c, d, e], gid: [f, g, h] }
	}

	pub(crate) fn into_raw(self) -> [u8; 8] {
		debug_assert!(self.permissions & 1 << 15 == 0, "high bit will be lost");
		let mut buf = [0; 8];
		buf[0..2].copy_from_slice(&(self.permissions << 1).to_le_bytes());
		buf[2..5].copy_from_slice(&self.uid);
		buf[5..8].copy_from_slice(&self.gid);
		buf
	}

	/// Get 24-bit uid as a 32-bit integer.
	pub fn uid(&self) -> u32 {
		let [a, b, c] = self.uid;
		u32::from_le_bytes([a, b, c, 0])
	}

	/// Get 24-bit gid as a 32-bit integer.
	pub fn gid(&self) -> u32 {
		let [a, b, c] = self.gid;
		u32::from_le_bytes([a, b, c, 0])
	}

	/// Set uid from a 32-bit integer.
	///
	/// # Panics
	///
	/// If `uid` does not fit in 24 bits.
	pub fn set_uid(&mut self, uid: u32) {
		let [a, b, c, d] = uid.to_le_bytes();
		assert!(d == 0, "uid does not fit in 24 bits");
		self.uid = [a, b, c];
	}

	/// Set gid from a 32-bit integer.
	///
	/// # Panics
	///
	/// If `gid` does not fit in 24 bits.
	pub fn set_gid(&mut self, gid: u32) {
		let [a, b, c, d] = gid.to_le_bytes();
		assert!(d == 0, "uid does not fit in 24 bits");
		self.gid = [a, b, c];
	}
}

#[cfg(any(test, fuzzing))]
impl<'a> arbitrary::Arbitrary<'a> for Unix {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		Ok(Self {
			permissions: u.int_in_range(u16::MIN..=u16::MAX)? >> 1,
			uid: u.arbitrary()?,
			gid: u.arbitrary()?,
		})
	}
}
