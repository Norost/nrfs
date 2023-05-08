use {
	super::{
		cipher::{Cipher, DecryptError},
		record::{Depth, RecordRef},
	},
	crate::{BlockSize, CipherType, Compression, KeyDerivation, MaxRecordSize},
	core::fmt,
	endian::u64le,
};

#[repr(C)]
pub(crate) struct FsHeader {
	pub magic: [u8; 4],
	pub version: u8,
	pub block_size: u8,
	pub cipher: u8,
	pub kdf: u8,
	pub kdf_parameters: [u8; 8],
	pub key_hash: [u8; 2],
	pub _reserved: [u8; 6],
	pub nonce: u64le,
	pub uid: [u8; 16],
	pub hash: [u8; 16],
}

raw!(FsHeader);

#[repr(C)]
pub(crate) struct FsInfo {
	pub configuration: Configuration,
	pub total_block_count: u64le,
	pub lba_offset: u64le,
	pub block_count: u64le,

	pub key1: [u8; 32],
	pub key2: [u8; 32],

	pub object_list_root: RecordRef,
	pub object_bitmap_root: RecordRef,
	pub allocation_log_head: RecordRef,
}

raw!(FsInfo);

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

impl FsHeader {
	/// The version of the on-disk format.
	pub const VERSION: u8 = 1;

	pub fn cipher(&self) -> Result<CipherType, u8> {
		CipherType::from_raw(self.cipher).ok_or(self.cipher)
	}

	pub fn block_size(&self) -> BlockSize {
		BlockSize::from_raw(self.block_size & 0xf).expect("invalid block size")
	}

	pub fn key_derivation(&self) -> Result<KeyDerivation, u8> {
		KeyDerivation::from_raw(self.kdf, &self.kdf_parameters).ok_or(self.kdf)
	}

	/// Verify the key.
	///
	/// Returns `true` if the key *may* be able to decrypt the header.
	pub fn verify_key(&self, key: &[u8; 32]) -> bool {
		use poly1305::universal_hash::KeyInit;
		let hasher = poly1305::Poly1305::new_from_slice(key).unwrap();
		let hash = hasher.compute_unpadded(&[0; 16]);
		let &[h0, h1, ..] = hash.as_slice() else { unreachable!() };
		u16::from_ne_bytes([h0, h1]) == u16::from_ne_bytes(self.key_hash)
	}

	/// Encrypt filesystem info.
	///
	/// # Panics
	///
	/// If `data` is too small to represent a full header, i.e. it is smaller than 448 bytes.
	pub fn encrypt(&mut self, key: &[u8; 32], data: &mut [u8; 512 - 64]) {
		let cipher = Cipher { ty: self.cipher().unwrap(), key1: *key, key2: [0; 32] };
		self.nonce += 1;
		self.hash = cipher.encrypt(&nonce(self.nonce, &self.uid), data);
	}

	/// Attempt to decrypt filesystem info.
	///
	/// # Panics
	///
	/// If `data` is too small to represent a full header, i.e. it is smaller than 448 bytes.
	pub fn decrypt<'d>(
		&self,
		key: &[u8; 32],
		data: &'d mut [u8; 512 - 64],
	) -> Result<(FsInfo, &'d [u8]), DecryptError> {
		let cipher = Cipher { ty: self.cipher().unwrap(), key1: *key, key2: [0; 32] };
		cipher.decrypt(&nonce(self.nonce, &self.uid), &self.hash, data)?;
		Ok(FsInfo::from_raw_slice(data).expect("data too small"))
	}

	pub fn hash_key(key: &[u8; 32]) -> [u8; 2] {
		use poly1305::universal_hash::KeyInit;
		let hash = poly1305::Poly1305::new_from_slice(key)
			.unwrap()
			.compute_unpadded(&[0; 16]);
		hash[..2].try_into().unwrap()
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
		MirrorCount::from_raw(self.get(0, 2) + 1).unwrap()
	}

	pub fn set_mirror_count(&mut self, value: MirrorCount) {
		self.set(0, 2, value.to_raw() - 1)
	}

	pub fn mirror_index(&self) -> MirrorIndex {
		MirrorIndex::from_raw(self.get(2, 2)).unwrap()
	}

	pub fn set_mirror_index(&mut self, value: MirrorIndex) {
		self.set(2, 2, value.to_raw())
	}

	pub fn max_record_size(&self) -> MaxRecordSize {
		MaxRecordSize::from_raw(self.get(4, 4) + 9).unwrap()
	}

	pub fn set_max_record_size(&mut self, value: MaxRecordSize) {
		self.set(4, 4, value.to_raw() - 9)
	}

	pub fn object_list_depth(&self) -> Depth {
		self.get(8, 2).try_into().unwrap()
	}

	pub fn set_object_list_depth(&mut self, value: Depth) {
		self.set(8, 2, value.into())
	}

	pub fn compression_level(&self) -> u8 {
		self.get(12, 4)
	}

	pub fn set_compression_level(&mut self, value: u8) {
		self.set(12, 4, value)
	}

	pub fn compression_algorithm(&self) -> Result<Compression, u8> {
		Compression::from_raw(self.0[2]).ok_or(self.0[2])
	}

	pub fn set_compression_algorithm(&mut self, value: Compression) {
		self.0[2] = value.to_raw()
	}
}

impl fmt::Debug for FsHeader {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut f = f.debug_struct(stringify!(FsHeader));
		f.field("magic", &String::from_utf8_lossy(&self.magic));
		f.field("version", &self.version);
		f.field("block_size", &self.block_size());
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
		f.finish()
	}
}

impl fmt::Debug for FsInfo {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut f = f.debug_struct(stringify!(FsInfo));
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

fn nonce(nonce: u64le, uid: &[u8; 16]) -> [u8; 24] {
	let mut b = [0; 24];
	b[..8].copy_from_slice(&u64::from(nonce).to_le_bytes());
	b[8..].copy_from_slice(uid);
	b
}
