#[cfg(miri)]
use xxhash_rust::const_xxh3::xxh3_128;
#[cfg(not(miri))]
use xxhash_rust::xxh3::xxh3_128;
use {
	chacha20::{
		cipher::{KeyIvInit as _, StreamCipher as _},
		XChaCha12,
	},
	poly1305::{universal_hash::KeyInit as _, Poly1305},
};

n2e! {
	[CipherType]
	0 NoneXxh3
	1 XChaCha12Poly1305
}

/// Generic cipher.
pub(crate) struct Cipher {
	pub key1: [u8; 32],
	pub key2: [u8; 32],
	pub ty: CipherType,
}

impl Cipher {
	/// Hash data.
	fn hash(&self, data: &[u8]) -> [u8; 16] {
		match self.ty {
			CipherType::NoneXxh3 => xxh3_128(data).to_le_bytes(),
			CipherType::XChaCha12Poly1305 => Poly1305::new_from_slice(&self.key1)
				.unwrap()
				.compute_unpadded(data)
				.into(),
		}
	}

	/// Apply keystream with the given key.
	fn apply_inner(&self, key: &[u8; 32], nonce: &[u8; 24], data: &mut [u8]) {
		match self.ty {
			CipherType::NoneXxh3 => {}
			CipherType::XChaCha12Poly1305 => XChaCha12::new_from_slices(key, nonce)
				.unwrap()
				.apply_keystream(data),
		}
	}

	/// Apply keystream.
	fn apply(&self, nonce: &[u8; 24], data: &mut [u8]) {
		self.apply_inner(&self.key1, nonce, data)
	}

	/// Apply keystream for metadata, such as record headers.
	pub fn apply_meta(&self, nonce: &[u8; 24], data: &mut [u8]) {
		self.apply_inner(&self.key2, nonce, data)
	}

	/// Decrypt data in-place.
	pub fn decrypt(
		&self,
		nonce: &[u8; 24],
		hash: &[u8; 16],
		data: &mut [u8],
	) -> Result<(), DecryptError> {
		if !ct_eq(*hash, self.hash(data)) {
			return Err(DecryptError);
		}
		self.apply(nonce, data);
		Ok(())
	}

	/// Encrypt data in-place.
	///
	/// Returns the hash.
	pub fn encrypt(&self, nonce: &[u8; 24], data: &mut [u8]) -> [u8; 16] {
		self.apply(nonce, data);
		self.hash(data)
	}
}

/// Error returned when something went wrong while decrypting.
#[derive(Clone, Debug)]
pub struct DecryptError;

/// Constant-time comparison.
fn ct_eq(lhs: [u8; 16], rhs: [u8; 16]) -> bool {
	u128::from_ne_bytes(lhs) == u128::from_ne_bytes(rhs)
}
