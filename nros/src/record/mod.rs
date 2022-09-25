macro_rules! n2e {
	{ [$name:ident] $($v:literal $k:ident)* } => {
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
};

pub use compression::Compression;

#[derive(Clone, Copy, Default)]
#[repr(C, align(64))]
pub struct Record {
	pub hash: [u8; 32],
	pub lba: u64le,
	pub length: u32le,
	pub hash_algorithm: u8,
	pub compression: u8,
	pub reference_count: u16le,
	pub total_length: u64le,
	pub _reserved: [u8; 8],
}

raw!(Record);

impl Record {
	pub fn pack(data: &[u8], buf: &mut [u8], compression: Compression) -> Record {
		let (compression, length) = compression.compress(data, buf);
		Self {
			length: length.into(),
			compression: compression.to_raw(),
			..Default::default()
		}
	}

	pub fn unpack(
		&self,
		data: &[u8],
		buf: &mut Vec<u8>,
		max_record_size: MaxRecordSize,
	) -> Result<(), UnpackError> {
		if data.len() > 1 << max_record_size.to_raw() {
			return Err(UnpackError::ExceedsRecordSize);
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
		let (a, b) = self.hash.split_at(16);
		let a = u128::from_be_bytes(a.try_into().unwrap());
		let b = u128::from_be_bytes(b.try_into().unwrap());
		let mut f = f.debug_struct(stringify!(Record));
		f.field("hash", &format_args!("{:016x}{:016x}", a, b));
		f.field("lba", &self.lba);
		f.field("length", &self.length);
		f.field("hash_algorithm", &self.hash_algorithm);
		if let Some(c) = Compression::from_raw(self.compression) {
			f.field("compression_algorithm", &c);
		} else {
			f.field("compression_algorithm", &self.compression);
		}
		f.field("total_length", &self.total_length);
		f.field("reference_count", &self.reference_count);
		f.finish()
	}
}

#[derive(Debug)]
pub enum UnpackError {
	ExceedsRecordSize,
	UnknownCompressionAlgorithm,
}

n2e! {
	[MaxRecordSize]
	10 K1
	11 K2
	12 K4
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
