#[derive(Clone, Copy, Debug, Default)]
pub struct MTime {
	pub mtime: i64,
}

impl MTime {
	pub(crate) fn from_raw(data: [u8; 8]) -> Self {
		Self { mtime: i64::from_le_bytes(data) >> 1 }
	}

	pub(crate) fn into_raw(self) -> [u8; 8] {
		(self.mtime << 1).to_le_bytes()
	}
}

#[cfg(any(test, fuzzing))]
impl<'a> arbitrary::Arbitrary<'a> for MTime {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		Ok(Self { mtime: u.int_in_range(i64::MIN..=i64::MAX)? >> 1 })
	}
}
