mod compression;

use {
	crate::{BlockSize, Cipher, RecordCipher, Resource},
	core::fmt,
	endian::u64le,
};

pub use compression::Compression;

#[derive(Clone, Copy, Default, PartialEq)]
#[repr(C)]
pub(crate) struct Record {
	pub lba: u64le,
	nonce: u64le,
	length: [u8; 3],
	compression: u8,
	hash: [u8; 12],
}

raw!(Record);

impl Record {
	pub fn pack(
		data: &[u8],
		buf: &mut [u8],
		compression: Compression,
		block_size: BlockSize,
		cipher: Cipher,
	) -> Record {
		debug_assert!(
			!data.is_empty(),
			"Record::pack should not be called with empty data"
		);

		let (compression, length) = compression.compress(data, buf, block_size);

		// Ensure we only encrypt *and hash* the blocks that contain the compressed data.
		let buf = &mut buf[..block_size.round_up(length.try_into().unwrap())];
		let (nonce, hash) = RecordCipher { cipher, real_len: length }.encrypt(buf);

		let [length @ .., d] = length.to_le_bytes();
		debug_assert_eq!(d, 0, "length does not fit in 24 bits");

		Self {
			lba: u64::MAX.into(),
			nonce: nonce.into(),
			length,
			compression: compression.to_raw(),
			hash,
		}
	}

	pub fn unpack<R: Resource>(
		&self,
		data: &mut [u8],
		resource: &R,
		max_record_size: MaxRecordSize,
		cipher: Cipher,
	) -> Result<R::Buf, UnpackError> {
		// TODO add a cipher type that only allows decryption
		// and takes a nonce as argument.
		debug_assert_eq!(self.nonce(), cipher.nonce, "nonce mismatch");

		let mut buf = resource.alloc();

		if data.is_empty() {
			debug_assert_eq!(self.length(), 0, "data empty but record length is nonzero");
			return Ok(buf);
		}

		RecordCipher { cipher, real_len: self.length_u32() }
			.decrypt(&self.hash, data)
			.map_err(|()| UnpackError::HashMismatch)?;

		let data = &data[..self.length()];
		self.compression()
			.map_err(|_| UnpackError::UnknownCompressionAlgorithm)?
			.decompress::<R>(data, &mut buf, 1 << max_record_size.to_raw())
			.then_some(())
			.ok_or(UnpackError::ExceedsRecordSize)?;

		Ok(buf)
	}

	fn length_u32(&self) -> u32 {
		let [a, b, c] = self.length;
		u32::from_le_bytes([a, b, c, 0])
	}

	pub fn length(&self) -> usize {
		usize::try_from(self.length_u32()).unwrap()
	}

	pub fn nonce(&self) -> u64 {
		self.nonce.into()
	}

	pub fn compression(&self) -> Result<Compression, u8> {
		Compression::from_raw(self.compression).ok_or(self.compression)
	}
}

impl fmt::Debug for Record {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut f = f.debug_struct(stringify!(Record));
		f.field("lba", &self.lba);
		f.field("nonce", &self.nonce);
		f.field("length", &self.length());
		let c = Compression::from_raw(self.compression);
		let c: &dyn fmt::Debug = if let Some(c) = c.as_ref() { c } else { &c };
		f.field("compression", c);
		let mut hash = [0; 16];
		hash[..12].copy_from_slice(&self.hash);
		f.field("hash", &format_args!("{:#x}", u128::from_le_bytes(hash)));
		f.finish()
	}
}

#[derive(Debug)]
pub enum UnpackError {
	ExceedsRecordSize,
	UnknownCompressionAlgorithm,
	HashMismatch,
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
