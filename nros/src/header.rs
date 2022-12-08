use {
	crate::{Compression, MaxRecordSize, Record},
	core::fmt,
	endian::{u16le, u32le, u64le},
};

#[repr(C, align(64))]
pub(crate) struct Header {
	pub magic: [u8; 16],

	pub version: [u8; 3],
	pub compression: u8,
	pub block_length_p2: u8,
	pub max_record_length_p2: u8,
	pub mirror_count: u8,
	pub mirror_index: u8,

	pub uid: [u8; 16],

	pub total_block_count: u64le,
	pub lba_offset: u64le,

	pub block_count: u64le,

	pub object_list: Record,

	pub allocation_log: Record,

	pub xxh3: u64le,
	pub generation: u64le,

	pub extra: [u8; 512 - 136],
}

raw!(Header);

impl Default for Header {
	fn default() -> Self {
		Self {
			magic: Self::MAGIC,

			version: Self::VERSION,
			compression: Compression::Lz4.to_raw(),
			block_length_p2: Default::default(),
			max_record_length_p2: MaxRecordSize::K128.to_raw(),
			mirror_count: Default::default(),
			mirror_index: Default::default(),

			uid: Default::default(),

			total_block_count: Default::default(),
			lba_offset: Default::default(),
			block_count: Default::default(),

			object_list: Default::default(),
			allocation_log: Default::default(),

			xxh3: Default::default(),
			generation: Default::default(),

			extra: [0; 512 - 136],
		}
	}
}

impl Header {
	/// The magic every header begins with.
	pub const MAGIC: [u8; 16] = *b"Nora Reliable FS";
	/// The version of the on-disk format.
	pub const VERSION: [u8; 3] = [0, 2, 0];

	/// Check if the magic is proper & the version is compatible.
	pub fn verify_compatible(&mut self) -> bool {
		self.magic == Self::MAGIC && self.version == Self::VERSION
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
		self.xxh3 = xxhash_rust::xxh3::xxh3_64(self.as_ref()).into();
	}

	/// Check whether two headers are part of the same filesystem.
	pub fn compatible(&self, other: &Self) -> bool {
		// Comparing other headers *shouldn't* be necessary unless
		// we're deliberately being screwed with - in which case all
		// bets are off anyways.
		self.uid == other.uid && self.generation == other.generation
	}
}

impl fmt::Debug for Header {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let mut f = f.debug_struct(stringify!(Header));

		// TODO use Utf8Lossy when it is stabilized.
		f.field("magic", &String::from_utf8_lossy(&self.magic));

		let [a, b, c] = self.version;
		f.field("version", &format_args!("v{}.{}.{}", a, b, c));
		if let Some(c) = Compression::from_raw(self.compression) {
			f.field("compression", &c);
		} else {
			f.field("compression", &self.compression);
		}
		f.field(
			"block_length_p2",
			&format_args!("2**{}", self.block_length_p2),
		);
		f.field(
			"max_record_length_p2",
			&format_args!("2**{}", self.max_record_length_p2),
		);
		f.field("mirror_count", &self.mirror_count);
		f.field("mirror_index", &self.mirror_count);

		f.field(
			"uid",
			&format_args!("{:016x}", u128::from_le_bytes(self.uid)),
		);

		f.field("total_block_count", &self.total_block_count);
		f.field("lba_offset", &self.lba_offset);
		f.field("block_count", &self.block_count);

		f.field("object_list", &self.object_list);

		f.field("allocation_log", &self.allocation_log);

		f.field("xxh3", &self.xxh3);
		f.field("generation", &self.generation);

		f.finish_non_exhaustive()
	}
}
