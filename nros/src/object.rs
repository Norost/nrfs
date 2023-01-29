use {crate::Record, endian::u64le};

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub(crate) struct Object {
	pub root: Record,
	pub total_length: u64le,
	pub block_count: u64le,
	pub reference_count: u64le,
	pub _reserved: u64,
}

raw!(Object);

impl Object {
	pub fn total_length(&self) -> u64 {
		self.total_length.into()
	}
}
