#[cfg(miri)]
use xxhash_rust::const_xxh3::xxh3_128;
#[cfg(not(miri))]
use xxhash_rust::xxh3::xxh3_128;
use {
	chacha20::{
		cipher::{KeyIvInit as _, StreamCipher as _},
		ChaCha8,
	},
	poly1305::{universal_hash::KeyInit as _, Poly1305},
};

n2e! {
	[CipherType]
	0 NoneXxh3
	1 ChaCha8Poly1305
}

/// Generic cipher.
pub(crate) struct Cipher {
	pub key: [u8; 32],
	pub ty: CipherType,
}

impl Cipher {
	/// Hash data.
	fn hash(&self, data: &[u8]) -> [u8; 16] {
		match self.ty {
			CipherType::NoneXxh3 => xxh3_128(data).to_le_bytes(),
			CipherType::ChaCha8Poly1305 => Poly1305::new_from_slice(&self.key)
				.unwrap()
				.compute_unpadded(data)
				.into(),
		}
	}

	/// Apply keystream.
	fn apply(&self, nonce: u64, data: &mut [u8]) {
		match self.ty {
			CipherType::NoneXxh3 => {}
			CipherType::ChaCha8Poly1305 => {
				let mut nonce12 = [0; 12];
				nonce12[..8].copy_from_slice(&nonce.to_le_bytes());
				ChaCha8::new_from_slices(&self.key, &nonce12)
					.unwrap()
					.apply_keystream(data)
			}
		}
	}

	/// Decrypt data in-place.
	pub fn decrypt(self, nonce: u64, hash: &[u8; 16], data: &mut [u8]) -> Result<(), DecryptError> {
		if !ct_eq(*hash, self.hash(data)) {
			return Err(DecryptError);
		}
		self.apply(nonce, data);
		Ok(())
	}

	/// Encrypt data in-place.
	///
	/// Returns the hash.
	pub fn encrypt(self, nonce: u64, data: &mut [u8]) -> [u8; 16] {
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
