use {
	crate::{Compression, MaxRecordSize, RecordTree},
	core::fmt,
	endian::{u16le, u32le, u64le},
};

#[repr(C, align(64))]
pub(crate) struct Header {
	pub magic: [u8; 16],

	pub version: u32le,
	pub block_length_p2: u8,
	pub max_record_length_p2: u8,
	pub compression: u8,
	pub mirror_count: u8,
	pub uid: u64le,

	pub total_block_count: u64le,
	pub lba_offset: u64le,

	pub block_count: u64le,
	pub _reserved_0: u64,

	pub object_list: RecordTree,

	pub allocation_log_lba: u64le,
	pub allocation_log_length: u64le,

	pub xxh3: u64le,
	pub generation: u32le,
	pub _reserved_1: u16,
	pub header_length: u16le,
}

raw!(Header);

impl Default for Header {
	fn default() -> Self {
		Self {
			magic: Self::MAGIC,

			version: 0x00_00_0003,
			block_length_p2: Default::default(),
			max_record_length_p2: MaxRecordSize::K128.to_raw(),
			compression: Compression::Lz4.to_raw(),
			mirror_count: Default::default(),
			uid: Default::default(),

			total_block_count: Default::default(),
			lba_offset: Default::default(),

			block_count: Default::default(),
			_reserved_0: Default::default(),

			object_list: Default::default(),

			allocation_log_lba: Default::default(),
			allocation_log_length: Default::default(),

			xxh3: Default::default(),
			generation: Default::default(),
			_reserved_1: Default::default(),
			header_length: 4096, // Should be sufficient for at least a while
		}
	}
}

impl Header {
	/// The magic every header begins with;
	pub const MAGIC: [u8; 16] = *b"Nora Reliabe FS";

	/// Check if the magic is proper
	pub fn verify_magic(&mut self) -> bool {
		self.magic == Self::MAGIC
	}

	/// Check if the header data is intact.
	pub fn verify_xxh3(&mut self) -> bool {
		let cur = self.xxh3;
		self.xxh3 = 0.into();
		let chk = xxhash_rust::xxh3::xxh3_64(self.as_ref());
		self.xxh3 = cur;
		u64::from(cur) == chk
	}

	/// Update the `xxh3` field.
	pub fn update_xxh3(&mut self) {
		self.xxh3 = 0.into();
		self.xxh3 = xxhash_rust::xxh3::xxh3_64(self.as_ref());
	}

	/// Check whether two headers are part of the same filesystem.
	pub fn compatible(&self, other: &Self) -> bool {
		self.block_length_p2 == other.block_length_p2
			&& self.max_record_length_p2 == other.max_record_length_p2
			&& self.mirror_count == other.mirror_count
			&& self.uid == other.uid
			&& self.total_block_count == other.total_block_count
			&& self.object_list == other.object_list
			&& self.allocation_log_lba == other.allocation_log_lba
			&& self.allocation_log_length == other.allocation_log_length
			&& self.generation == other.generation
			&& self.header_length == other.header_length
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
		f.field(
			"block_length_p2",
			&format_args!("2**{}", self.block_length_p2),
		);
		f.field(
			"max_record_length_p2",
			&format_args!("2**{}", self.max_record_length_p2),
		);
		if let Some(c) = Compression::from_raw(self.compression) {
			f.field("compression", &c);
		} else {
			f.field("compression", &self.compression);
		}
		f.field("mirror_count", &self.mirror_count);
		f.field("uid", &format_args!("{:08x}", self.uid));

		f.field("total_block_count", &self.total_block_count);
		f.field("lba_offset", &self.lba_offset);

		f.field("block_count", &self.block_count);

		f.field("object_list", &self.object_list);

		f.field("allocation_log_lba", &self.allocation_log_lba);
		f.field("allocation_log_length", &self.allocation_log_length);

		f.field("xxh3", &self.xxh3);
		f.field("generation", &self.generation);
		f.field("header_length", &self.header_length);

		f.finish_non_exhaustive()
	}
}
