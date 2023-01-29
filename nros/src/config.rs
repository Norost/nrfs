use {
	crate::{BlockSize, CipherType, Compression, Dev, MaxRecordSize, Resource},
	core::num::{NonZeroU32, NonZeroU8},
};

/// Algorithm to derive key with.
pub enum KeyDeriver<'a> {
	/// No key derivation.
	/// Use a key directly.
	None { key: &'a [u8; 32] },
	/// Derive key from applying Argon2id to a password.
	Argon2id { password: &'a [u8], m: NonZeroU32, t: NonZeroU32, p: NonZeroU8 },
}

/// Configuration to create a new object store.
pub struct NewConfig<'a, D: Dev, R: Resource> {
	/// Memory & computation resources.
	pub resource: R,
	/// Mirror of chains of devices.
	pub mirrors: Vec<Vec<D>>,
	/// Magic to add to header.
	pub magic: [u8; 8],
	/// Key derivation algorithm to apply, with parameters.
	pub key_deriver: KeyDeriver<'a>,
	/// Hash & encryption algorithm to apply to filesystem.
	pub cipher: CipherType,
	/// Block size to use.
	///
	/// Should be at least as large as the largest *physical* block size of the devices.
	pub block_size: BlockSize,
	/// Maximum size of a record.
	///
	/// Must be at least as large as a block.
	pub max_record_size: MaxRecordSize,
	/// Compression to apply.
	pub compression: Compression,
	/// Size of the cache.
	///
	/// This is a soft limit.
	/// Real usage may exceed this.
	pub cache_size: usize,
}

/// Key or password to decrypt the header.
pub enum KeyPassword<'a> {
	Key(&'a [u8; 32]),
	Password(&'a [u8]),
}

/// Configuration to load an existing object store.
pub struct LoadConfig<'a, D: Dev, R: Resource> {
	/// Memory & computation resources.
	pub resource: R,
	/// Devices.
	pub devices: Vec<D>,
	/// Magic which the header should match.
	pub magic: [u8; 8],
	/// Key or password
	pub key_password: KeyPassword<'a>,
	/// Size of the cache.
	///
	/// This is a soft limit.
	/// Real usage may exceed this.
	pub cache_size: usize,
	/// Whether to allow repair of this filesystem.
	///
	/// If `false`, errors will not be corrected.
	pub allow_repair: bool,
	/// Method to retrieve either a key directly or get a password.
	///
	/// If the passed parameter is `true` a password is expected.
	pub retrieve_key: &'a mut dyn FnMut(bool) -> Vec<u8>,
}
