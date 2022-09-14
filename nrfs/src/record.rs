use {
	crate::{
		storage::{Read, Storage},
		Error,
	},
	alloc::vec::Vec,
	core::{fmt, time::Duration},
	endian::{u32le, u64le},
};

pub const COMPRESSION_NONE: u8 = 0;

pub const HASH_NONE: u8 = 0;

#[derive(Clone, Copy, Default)]
#[repr(C, align(64))]
pub struct Record {
	pub hash: [u8; 32],
	pub lba: u64le,
	pub length: u32le,
	pub hash_algorithm: u8,
	pub compression_algorithm: u8,
	pub ty: u8,
	pub flags: u8,
	pub total_length: u64le,
	pub modification_time: [u8; 7],
	pub reference_count: u8,
}

raw!(Record);

impl Record {
	pub fn pack(data: &[u8], buf: &mut [u8]) -> Result<Record, PackError> {
		buf.get_mut(..data.len())
			.ok_or(PackError::BufTooSmall)?
			.copy_from_slice(data);
		Ok(Self { length: (data.len() as u32).into(), ..Default::default() })
	}

	pub fn unpack(
		&self,
		data: &[u8],
		buf: &mut Vec<u8>,
		max_record_size_p2: u8,
	) -> Result<(), UnpackError> {
		buf.resize(1 << max_record_size_p2, 0);
		Ok(match self.compression_algorithm {
			COMPRESSION_NONE => buf
				.get_mut(..data.len())
				.ok_or(UnpackError::BufTooSmall)?
				.copy_from_slice(data),
			_ => return Err(UnpackError::UnknownCompressionAlgorithm),
		})
	}
}

impl fmt::Debug for Record {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let [a, b, c, d, e, g, h] = self.modification_time;
		let t = u64::from_le_bytes([a, b, c, d, e, g, h, 0]);
		let t = Duration::from_millis(t);
		let (a, b) = self.hash.split_at(16);
		let a = u128::from_be_bytes(a.try_into().unwrap());
		let b = u128::from_be_bytes(b.try_into().unwrap());
		f.debug_struct(stringify!(Record))
			.field("hash", &format_args!("{:016x}{:016x}", a, b))
			.field("lba", &self.lba)
			.field("length", &self.length)
			.field("hash_algorithm", &self.hash_algorithm)
			.field("compression_algorithm", &self.compression_algorithm)
			.field("ty", &self.ty)
			.field("flags", &format_args!("{:08b}", self.flags))
			.field("total_length", &self.total_length)
			.field("modification_time", &t)
			.field("reference_count", &self.reference_count)
			.finish()
	}
}

#[derive(Debug)]
pub enum PackError {
	BufTooSmall,
}

#[derive(Debug)]
pub enum UnpackError {
	BufTooSmall,
	UnknownCompressionAlgorithm,
}
