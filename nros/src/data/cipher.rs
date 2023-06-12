#[cfg(miri)]
use xxhash_rust::const_xxh3::xxh3_128;
#[cfg(not(miri))]
use xxhash_rust::xxh3::xxh3_128;
use {
	chacha20::{
		cipher::{KeyIvInit as _, StreamCipher as _},
		XChaCha12,
	},
	chacha20poly1305::{
		aead::{AeadInPlace, NewAead},
		Tag, XChaCha12Poly1305, XNonce,
	},
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
	/// Apply keystream for metadata, such as record headers.
	pub fn apply_meta(&self, nonce: &[u8; 24], data: &mut [u8]) {
		match self.ty {
			CipherType::NoneXxh3 => {}
			CipherType::XChaCha12Poly1305 => XChaCha12::new_from_slices(&self.key2, nonce)
				.unwrap()
				.apply_keystream(data),
		}
	}

	/// Decrypt data in-place.
	pub fn decrypt(
		&self,
		nonce: &[u8; 24],
		hash: &[u8; 16],
		data: &mut [u8],
	) -> Result<(), DecryptError> {
		match self.ty {
			CipherType::NoneXxh3 => (&xxh3_128(data).to_le_bytes() == hash)
				.then_some(())
				.ok_or(DecryptError),
			CipherType::XChaCha12Poly1305 => XChaCha12Poly1305::new_from_slice(&self.key1)
				.unwrap()
				.decrypt_in_place_detached(
					XNonce::from_slice(nonce),
					&[],
					data,
					Tag::from_slice(hash),
				)
				.map_err(|_| DecryptError),
		}
	}

	/// Encrypt data in-place.
	///
	/// Returns the hash.
	pub fn encrypt(&self, nonce: &[u8; 24], data: &mut [u8]) -> [u8; 16] {
		match self.ty {
			CipherType::NoneXxh3 => xxh3_128(data).to_le_bytes(),
			CipherType::XChaCha12Poly1305 => XChaCha12Poly1305::new_from_slice(&self.key1)
				.unwrap()
				.encrypt_in_place_detached(XNonce::from_slice(nonce), &[], data)
				.unwrap()
				.into(),
		}
	}
}

/// Error returned when something went wrong while decrypting.
#[derive(Clone, Debug)]
pub struct DecryptError;
