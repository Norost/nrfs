mod compression;

use {
	super::cipher::Cipher,
	crate::{resource::Buf, BlockSize},
	core::{fmt, iter::Step},
	endian::{u32le, u64le},
};

pub use compression::Compression;

/// Reference to a record.
#[derive(Clone, Copy, Default, PartialEq)]
#[repr(C)]
pub(crate) struct RecordRef(u64le);

pub const HEADER_LEN: u8 = 64;

impl RecordRef {
	/// A record reference that points to no data.
	pub const NONE: Self = Self(u64le::new(0));

	/// Create a new [`RecordRef`]
	///
	/// Should be used in combination with [`pack`] after picking a LBA.
	///
	/// # Panics
	///
	/// If either `lba` is out of range.
	pub fn new(lba: u64, blocks: u16) -> Self {
		assert!(lba < 0x1_0000_0000_0000, "lba out of range");
		Self((lba << 16 | u64::from(blocks)).into())
	}

	pub fn lba(&self) -> u64 {
		u64::from(self.0) >> 16
	}

	pub fn blocks(&self) -> u16 {
		u64::from(self.0) as u16
	}
}

raw!(RecordRef);

impl fmt::Debug for RecordRef {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut f = f.debug_struct(stringify!(RecordRef));
		f.field("lba", &self.lba());
		f.field("blocks", &self.blocks());
		f.finish()
	}
}

/// Header attached to all records.
#[derive(Clone, Copy, Default, PartialEq)]
#[repr(C)]
struct RecordHeader {
	nonce: [u8; 24],
	length: u32le,
	_reserved: [u8; 4 + 8 + 7],
	compression: u8,
	hash: [u8; 16],
}

raw!(RecordHeader);

const _: () = assert!(core::mem::size_of::<RecordHeader>() == HEADER_LEN as _);

impl RecordHeader {
	pub fn compression(&self) -> Result<Compression, u8> {
		Compression::from_raw(self.compression).ok_or(self.compression)
	}
}

impl fmt::Debug for RecordHeader {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut f = f.debug_struct(stringify!(RecordHeader));
		let nonce_l = u128::from_le_bytes(self.nonce[..16].try_into().unwrap());
		let nonce_h = u64::from_le_bytes(self.nonce[16..].try_into().unwrap());
		f.field("nonce", &format_args!("{:#018x}{:032x}", nonce_h, nonce_l));
		f.field("length", &self.length);
		let c = Compression::from_raw(self.compression);
		let c: &dyn fmt::Debug = if let Some(c) = c.as_ref() { c } else { &c };
		f.field("compression", c);
		let hash = u128::from_le_bytes(self.hash);
		f.field("hash", &format_args!("{:#034x}", hash));
		f.finish()
	}
}

#[derive(Debug)]
pub enum UnpackError {
	ExceedsRecordSize,
	UnknownCompressionAlgorithm,
	HashMismatch,
	BadLength,
}

n2e! {
	[MaxRecordSize]
	9 B512
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
}

impl Default for MaxRecordSize {
	fn default() -> Self {
		Self::K128
	}
}

/// Pack data into a record.
///
/// Returns the length of the record in blocks.
pub(crate) fn pack(
	data: &[u8],
	buf: &mut [u8],
	compression: Compression,
	block_size: BlockSize,
	cipher: Cipher,
	nonce: &[u8; 24],
) -> u16 {
	debug_assert!(
		!data.is_empty(),
		"Record::pack should not be called with empty data"
	);
	debug_assert_eq!(
		buf.len() % (1 << block_size.to_raw()),
		0,
		"buf not a multiple of block size"
	);

	let (header, buf) = buf.split_at_mut(HEADER_LEN.into());

	let (compression, len) = compression.compress(HEADER_LEN.into(), data, buf, block_size);

	let blocks = block_size
		.min_blocks((u32::from(HEADER_LEN) + len).try_into().unwrap())
		.try_into()
		.unwrap();
	let buf =
		&mut buf[..(1 << block_size.to_raw()) * usize::from(blocks) - usize::from(HEADER_LEN)];

	let hash = cipher.encrypt(nonce, buf);

	header.copy_from_slice(
		RecordHeader {
			nonce: *nonce,
			length: len.into(),
			_reserved: [0; 19],
			compression: compression.to_raw(),
			hash,
		}
		.as_ref(),
	);

	let (nonce, hdr) = header.split_array_mut();
	cipher.apply_meta(nonce, hdr);
	blocks
}

/// Unpack data from a record.
pub(crate) fn unpack<B: Buf>(
	data: &mut [u8],
	mut buf: B,
	max_record_size: MaxRecordSize,
	cipher: Cipher,
) -> Result<B, UnpackError> {
	let (header_raw, data) = data.split_at_mut(HEADER_LEN.into());

	let (nonce, hdr) = header_raw.split_array_mut();
	cipher.apply_meta(nonce, hdr);

	let mut header = RecordHeader::default();
	header.as_mut().copy_from_slice(header_raw);

	buf.resize(0, 0);

	cipher
		.decrypt(&header.nonce, &header.hash, data)
		.map_err(|_| UnpackError::HashMismatch)?;

	let data = data
		.get_mut(..u32::from(header.length).try_into().unwrap())
		.ok_or(UnpackError::BadLength)?;

	header
		.compression()
		.map_err(|_| UnpackError::UnknownCompressionAlgorithm)?
		.decompress::<B>(data, &mut buf, 1 << max_record_size.to_raw())
		.then_some(())
		.ok_or(UnpackError::ExceedsRecordSize)?;

	Ok(buf)
}

/// The depth of a record tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Depth {
	D0 = 0,
	D1 = 1,
	D2 = 2,
	D3 = 3,
}

impl Depth {
	fn i2d(i: usize) -> Option<Self> {
		[Self::D0, Self::D1, Self::D2, Self::D3].get(i).copied()
	}

	pub fn try_next(self) -> Option<Self> {
		Self::forward_checked(self, 1)
	}

	/// # Panics
	///
	/// If depth is already at the maximum.
	pub fn next(self) -> Self {
		Self::forward_checked(self, 1).expect("depth already at maximum")
	}

	/// # Panics
	///
	/// If depth is already at the minimum.
	pub fn prev(self) -> Self {
		Self::backward_checked(self, 1).expect("depth already at minimum")
	}
}

impl TryFrom<u8> for Depth {
	type Error = ();

	fn try_from(depth: u8) -> Result<Self, ()> {
		use Depth::*;
		[D0, D1, D2, D3].get(usize::from(depth)).copied().ok_or(())
	}
}

impl From<Depth> for u8 {
	fn from(depth: Depth) -> Self {
		depth as _
	}
}

impl Step for Depth {
	fn steps_between(start: &Self, end: &Self) -> Option<usize> {
		(*end as usize).checked_sub(*start as usize)
	}

	fn forward_checked(start: Self, count: usize) -> Option<Self> {
		(start as usize).checked_add(count).and_then(Self::i2d)
	}

	fn backward_checked(start: Self, count: usize) -> Option<Self> {
		(start as usize).checked_sub(count).and_then(Self::i2d)
	}
}
