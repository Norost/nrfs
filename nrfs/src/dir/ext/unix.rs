#[derive(Clone, Copy, Debug, Default)]
pub struct Entry {
	pub permissions: u16,
	pub uid: u32,
	pub gid: u32,
}

impl Entry {
	pub(crate) fn from_raw(data: [u8; 8]) -> Self {
		let [a, b, c, d, e, f, g, h] = data;
		Self {
			permissions: u16::from_le_bytes([a, b]),
			uid: u32::from_le_bytes([c, d, e, 0]),
			gid: u32::from_le_bytes([f, g, h, 0]),
		}
	}

	pub(crate) fn into_raw(self) -> [u8; 8] {
		let mut buf = [0; 8];
		buf[0..2].copy_from_slice(&self.permissions.to_le_bytes());
		buf[2..5].copy_from_slice(&self.uid.to_le_bytes()[..3]);
		buf[5..8].copy_from_slice(&self.gid.to_le_bytes()[..3]);
		buf
	}
}
