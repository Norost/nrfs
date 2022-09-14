use {
	crate::{record::Record, RecordTree},
	core::{fmt, mem},
	endian::u32le,
};

#[repr(C, align(64))]
pub struct Header {
	pub magic: [u8; 16],
	pub version: u32le,
	pub hash_algorithm: u8,
	pub compression_algorithm: u8,
	pub max_record_length_p2: u8,
	pub block_length_p2: u8,
	pub _reserved: [u64; 5],
	pub object_list: RecordTree,
	pub allocation_log: RecordTree,
}

raw!(Header);

impl Default for Header {
	fn default() -> Self {
		Self {
			magic: *b"Nora Reliable FS",
			version: 0x00_00_0000.into(),
			hash_algorithm: 0,
			compression_algorithm: 0,
			max_record_length_p2: 17,
			block_length_p2: 9,
			_reserved: [0; 5],
			object_list: RecordTree::default(),
			allocation_log: RecordTree::default(),
		}
	}
}

impl fmt::Debug for Header {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let v = u32::from(self.version);
		let (a, b, c) = (v >> 24, v >> 16 & 0xff, v & 0xffff);
		f.debug_struct(stringify!(Header))
			// TODO use Utf8Lossy when it is stabilized.
			.field("magic", &String::from_utf8_lossy(&self.magic))
			.field("version", &format_args!("v{}.{}.{}", a, b, c))
			.field("hash_algorithm", &self.hash_algorithm)
			.field("compression_algorithm", &self.compression_algorithm)
			.field(
				"max_record_length_p2",
				&format_args!("2**{}", self.max_record_length_p2),
			)
			.field(
				"block_length_p2",
				&format_args!("2**{}", self.block_length_p2),
			)
			.field("object_list", &self.object_list)
			.field("allocation_log", &self.allocation_log)
			.finish_non_exhaustive()
	}
}