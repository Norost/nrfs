macro_rules! n2e {
	{
		$(#[doc = $doc:literal])*
		[$name:ident]
		$($v:literal $k:ident)*
	} => {
		$(#[doc = $doc])*
		#[derive(Clone, Copy, Debug)]
		pub enum $name {
			$($k = $v,)*
		}

		impl $name {
			pub(crate) fn from_raw(n: u8) -> Option<Self> {
				Some(match n {
					$($v => Self::$k,)*
					_ => return None,
				})
			}

			pub(crate) fn to_raw(self) -> u8 {
				self as _
			}
		}
	};
}

mod compression;

use {
	alloc::vec::Vec,
	core::fmt,
	endian::{u16le, u32le, u64le},
	xxhash_rust::xxh3::xxh3_64,
};

pub use compression::Compression;

#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
pub struct Record {
	pub lba: u64le,
	pub length: u32le,
	pub compression: u8,
	pub _reserved: u8,
	pub references: u16le,
	pub xxh3: u64le,
	pub total_length: u64le,
}

raw!(Record);

impl Record {
	pub fn pack(data: &[u8], buf: &mut [u8], compression: Compression) -> Record {
		let (compression, length) = compression.compress(data, buf);
		Self {
			length: length.into(),
			compression: compression.to_raw(),
			// Zero out hash to allow zero optimization ("sparse objects")
			xxh3: if buf.is_empty() {
				0
			} else {
				xxh3_64(&buf[..length as _])
			}
			.into(),
			..Default::default()
		}
	}

	pub fn unpack(
		&self,
		data: &[u8],
		buf: &mut Vec<u8>,
		max_record_size: MaxRecordSize,
	) -> Result<(), UnpackError> {
		debug_assert_eq!(data.len() as u32, self.length);
		if data.len() > 1 << max_record_size.to_raw() {
			return Err(UnpackError::ExceedsRecordSize);
		}
		if !data.is_empty() && xxh3_64(data) != self.xxh3 {
			return Err(UnpackError::Xxh3Mismatch);
		}
		buf.clear();
		Compression::from_raw(self.compression)
			.ok_or(UnpackError::UnknownCompressionAlgorithm)?
			.decompress(data, buf, 1 << max_record_size.to_raw())
			.then_some(())
			.ok_or(UnpackError::ExceedsRecordSize)
	}
}

impl fmt::Debug for Record {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut f = f.debug_struct(stringify!(Record));
		f.field("lba", &self.lba);
		f.field("length", &self.length);
		if let Some(c) = Compression::from_raw(self.compression) {
			f.field("compression_algorithm", &c);
		} else {
			f.field("compression_algorithm", &self.compression);
		}
		f.field("total_length", &self.total_length);
		f.field("xxh3", &self.xxh3);
		f.field("references", &self.references);
		f.finish()
	}
}

#[derive(Debug)]
pub enum UnpackError {
	ExceedsRecordSize,
	UnknownCompressionAlgorithm,
	Xxh3Mismatch,
}

n2e! {
	/// Records sizes are at least 8 KiB to ensure the depth of a tree does
	/// not exceed 5 levels.
	[MaxRecordSize]
	13 K8
	14 K16
	15 K32
	16 K64
	17 K128
	18 K256
	19 K512
	20 M1
	21 M2
	22 M4
	23 M8
	24 M16
	25 M32
	26 M64
	27 M128
	28 M256
	29 M512
	30 G1
	31 G2
}
