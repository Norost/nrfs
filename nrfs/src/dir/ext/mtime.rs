#[derive(Clone, Copy, Debug, Default)]
pub struct Entry {
	pub mtime: i64,
}

impl Entry {
	pub(crate) fn from_raw(data: [u8; 8]) -> Self {
		Self { mtime: i64::from_le_bytes(data) }
	}

	pub(crate) fn into_raw(self) -> [u8; 8] {
		self.mtime.to_le_bytes()
	}
}
