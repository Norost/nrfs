use {
	crate::{
		BlockSize, CipherType, Compression, HeaderCipher, KeyDerivation, MaxRecordSize, Record,
	},
	core::{fmt, mem},
	endian::u64le,
};

#[derive(Default)]
#[repr(C)]
pub(crate) struct Header {
	pub magic: [u8; 4],
	pub version: u8,
	pub cipher: u8,
	pub _reserved: [u8; 4],
	pub key_derivation: [u8; 14],
	pub nonce: u64le,
	pub uid: [u8; 16],
	pub hash: [u8; 16],

	pub configuration: Configuration,
	pub total_block_count: u64le,
	pub lba_offset: u64le,
	pub block_count: u64le,
	pub key: [u8; 32],

	pub object_list_root: Record,
	pub object_bitmap_root: Record,
	pub allocation_log_head: Record,
}

#[derive(Default)]
#[repr(transparent)]
pub(crate) struct Configuration([u8; 8]);

n2e! {
	[MirrorCount]
	1 C1
	2 C2
	3 C3
	4 C4
}

n2e! {
	[MirrorIndex]
	0 I0
	1 I1
	2 I2
	3 I3
}

raw!(Header);

impl Header {
	/// The version of the on-disk format.
	pub const VERSION: u8 = 0;

	pub fn cipher(&self) -> Result<CipherType, u8> {
		CipherType::from_raw(self.cipher).ok_or(self.cipher)
	}

	pub fn get_cipher(data: &[u8]) -> Result<CipherType, u8> {
		assert!(data.len() >= 512);
		CipherType::from_raw(data[5]).ok_or(data[5])
	}

	pub fn key_derivation(&self) -> Result<KeyDerivation, u8> {
		KeyDerivation::from_raw(&self.key_derivation).ok_or(self.key_derivation[0])
	}

	pub fn get_key_derivation(data: &[u8]) -> Result<KeyDerivation, u8> {
		assert!(data.len() >= 512);
		let kdf = data[10..24].try_into().unwrap();
		KeyDerivation::from_raw(&kdf).ok_or(kdf[0])
	}

	pub fn get_nonce(data: &[u8]) -> u64 {
		assert!(data.len() >= 512);
		u64::from_le_bytes(data[24..32].try_into().unwrap())
	}

	pub fn get_uid(data: &[u8]) -> [u8; 16] {
		assert!(data.len() >= 512);
		data[32..48].try_into().unwrap()
	}

	/// Attempt to decrypt the header data with the given key.
	///
	/// # Panics
	///
	/// If `header` length is smaller than 512.
	pub fn decrypt(data: &mut [u8], cipher: HeaderCipher) -> Result<Self, ()> {
		assert!(data.len() >= 512, "header too small");

		let mut h = Self::default();
		h.as_mut()[..64].copy_from_slice(&data[..64]);

		cipher.decrypt(&h.hash, &mut data[64..])?;

		h.as_mut()[64..].copy_from_slice(&data[64..mem::size_of::<Self>()]);

		Ok(h)
	}

	/// Encrypt the header data with the given key.
	///
	/// Returns the new nonce.
	///
	/// # Panics
	///
	/// If `header` length is smaller than 512.
	#[must_use = "nonce must be used"]
	pub fn encrypt(&mut self, buf: &mut [u8], cipher: HeaderCipher) -> u64 {
		assert!(buf.len() >= 512, "header too small");

		buf[64..mem::size_of::<Self>()].copy_from_slice(&self.as_ref()[64..]);

		let (nonce, hash) = cipher.encrypt(&mut buf[64..]);
		(self.nonce, self.hash) = (nonce.into(), hash);

		buf[..64].copy_from_slice(&self.as_ref()[..64]);

		self.nonce.into()
	}
}

impl Configuration {
	fn get(&self, i: u8, bits: u8) -> u8 {
		let (i, shift) = (usize::from(i / 8), i % 8);
		let mask = (1 << bits) - 1;
		(self.0[i] >> shift) & mask
	}

	fn set(&mut self, i: u8, bits: u8, value: u8) {
		let (i, shift) = (usize::from(i / 8), i % 8);
		let mask = (1 << bits) - 1;
		debug_assert_eq!(value & !mask, 0, "value out of range");
		self.0[i] &= !(mask << shift);
		self.0[i] |= value << shift;
	}

	pub fn mirror_count(&self) -> MirrorCount {
		MirrorCount::from_raw(self.get(4, 2) + 1).unwrap()
	}

	pub fn set_mirror_count(&mut self, value: MirrorCount) {
		self.set(4, 2, value.to_raw() - 1)
	}

	pub fn mirror_index(&self) -> MirrorIndex {
		MirrorIndex::from_raw(self.get(6, 2)).unwrap()
	}

	pub fn set_mirror_index(&mut self, value: MirrorIndex) {
		self.set(6, 2, value.to_raw())
	}

	pub fn block_size(&self) -> BlockSize {
		BlockSize::from_raw(self.get(8, 4) + 9).unwrap()
	}

	pub fn set_block_size(&mut self, value: BlockSize) {
		self.set(8, 4, value.to_raw() - 9)
	}

	pub fn max_record_size(&self) -> MaxRecordSize {
		MaxRecordSize::from_raw(self.get(12, 4) + 9).unwrap()
	}

	pub fn set_max_record_size(&mut self, value: MaxRecordSize) {
		self.set(12, 4, value.to_raw() - 9)
	}

	pub fn object_list_depth(&self) -> u8 {
		self.get(16, 4)
	}

	pub fn set_object_list_depth(&mut self, value: u8) {
		self.set(16, 4, value)
	}

	pub fn compression_level(&self) -> u8 {
		self.get(20, 4)
	}

	pub fn set_compression_level(&mut self, value: u8) {
		self.set(20, 4, value)
	}

	pub fn compression_algorithm(&self) -> Result<Compression, u8> {
		let v = self.get(20, 4);
		Compression::from_raw(v).ok_or(v)
	}

	pub fn set_compression_algorithm(&mut self, value: Compression) {
		self.0[3] = value.to_raw()
	}
}

impl fmt::Debug for Header {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut f = f.debug_struct(stringify!(Header));
		// TODO use Utf8Lossy when it is stabilized.
		f.field("magic", &String::from_utf8_lossy(&self.magic));
		f.field("version", &self.version);
		fmt_either(&mut f, "cipher", self.cipher());
		fmt_either(&mut f, "key_derivation", self.key_derivation());
		f.field(
			"uid",
			&format_args!("{:#34x}", &u128::from_le_bytes(self.uid)),
		);
		f.field("nonce", &self.nonce);
		f.field(
			"hash",
			&format_args!("{:#34x}", &u128::from_le_bytes(self.hash)),
		);
		f.field("configuration", &self.configuration);
		f.field("total_block_count", &self.total_block_count);
		f.field("lba_offset", &self.lba_offset);
		f.field("block_count", &self.block_count);
		f.field("key", &format_args!("..."));
		f.field("object_list_root", &self.object_list_root);
		f.field("object_bitmap_root", &self.object_bitmap_root);
		f.field("allocation_log_head", &self.allocation_log_head);
		f.finish_non_exhaustive()
	}
}

impl fmt::Debug for Configuration {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut f = f.debug_struct(stringify!(Configuration));
		f.field("mirror_count", &self.mirror_count());
		f.field("mirror_index", &self.mirror_index());
		f.field("block_size", &self.block_size());
		f.field("max_record_size", &self.max_record_size());
		f.field("object_list_depth", &self.object_list_depth());
		f.field("compression_level", &self.compression_level());
		fmt_either(
			&mut f,
			"compression_algorithm",
			self.compression_algorithm(),
		);
		f.finish()
	}
}

fn fmt_either(f: &mut fmt::DebugStruct<'_, '_>, name: &str, x: Result<impl fmt::Debug, u8>) {
	f.field(
		name,
		x.as_ref()
			.map_or_else(|x| x as &dyn fmt::Debug, |x| x as &dyn fmt::Debug),
	);
}
