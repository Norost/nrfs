use crate::{BlockSize, CipherType, Compression, Dev, KeyDeriver, KeyPassword, MaxRecordSize};

pub struct NewConfig<'a, D: Dev> {
	/// Mirror of chains of devices.
	pub mirrors: Vec<Vec<D>>,
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

pub struct LoadConfig<'a, D: Dev> {
	/// Devices.
	pub devices: Vec<D>,
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
	/// If the passed parameter is `true` a password can be provided.
	///
	/// On failure, return `None`.
	pub retrieve_key: &'a mut dyn FnMut(bool) -> Option<KeyPassword>,
}
