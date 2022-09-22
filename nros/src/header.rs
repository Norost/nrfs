use {
	crate::{Compression, RecordTree},
	core::fmt,
	endian::{u32le, u64le},
};

#[repr(C, align(64))]
pub struct Header {
	pub magic: [u8; 16],
	pub version: u32le,
	pub hash_algorithm: u8,
	pub compression: u8,
	pub max_record_length_p2: u8,
	pub block_length_p2: u8,
	pub allocation_log_lba: u64le,
	pub allocation_log_length: u64le,
	pub _reserved: [u64; 3],
	pub object_list: RecordTree,
}

raw!(Header);

impl Default for Header {
	fn default() -> Self {
		Self {
			magic: *b"Nora Reliable FS",
			version: 0x00_00_0001.into(),
			hash_algorithm: 0,
			compression: 0,
			max_record_length_p2: 17,
			block_length_p2: 9,
			allocation_log_lba: 0.into(),
			allocation_log_length: 0.into(),
			_reserved: [0; 3],
			object_list: RecordTree::default(),
		}
	}
}

impl fmt::Debug for Header {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let v = u32::from(self.version);
		let (a, b, c) = (v >> 24, v >> 16 & 0xff, v & 0xffff);
		let mut f = f.debug_struct(stringify!(Header));
		// TODO use Utf8Lossy when it is stabilized.
		f.field("magic", &String::from_utf8_lossy(&self.magic));
		f.field("version", &format_args!("v{}.{}.{}", a, b, c));
		f.field("hash_algorithm", &self.hash_algorithm);
		if let Some(c) = Compression::from_raw(self.compression) {
			f.field("compression", &c);
		} else {
			f.field("compression", &self.compression);
		}
		f.field(
			"max_record_length_p2",
			&format_args!("2**{}", self.max_record_length_p2),
		);
		f.field(
			"block_length_p2",
			&format_args!("2**{}", self.block_length_p2),
		);
		f.field("allocation_log_lba", &self.allocation_log_lba);
		f.field("allocation_log_length", &self.allocation_log_length);
		f.field("object_list", &self.object_list);
		f.finish_non_exhaustive()
	}
}
