use {
	chacha20::{
		cipher::{KeyIvInit as _, StreamCipher as _},
		ChaCha8,
	},
	poly1305::{universal_hash::KeyInit as _, Poly1305},
	xxhash_rust::xxh3::xxh3_128,
};

n2e! {
	[CipherType]
	0 NoneXxh3
	1 ChaCha8Poly1305
}

/// Generic cipher.
pub(crate) struct Cipher {
	pub key: [u8; 32],
	pub nonce: u64,
	pub ty: CipherType,
}

/// Cipher for encrypting header data.
pub(crate) struct HeaderCipher {
	pub cipher: Cipher,
}

/// Cipher for encrypting record data.
pub(crate) struct RecordCipher {
	pub cipher: Cipher,
	pub real_len: u32,
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

	/// Decrypt data in-place with the given key.
	///
	/// # Warning
	///
	/// This does *not* check the hash!
	fn decrypt(self, data: &mut [u8]) {
		match self.ty {
			CipherType::NoneXxh3 => {}
			CipherType::ChaCha8Poly1305 => {
				let mut nonce = [0; 12];
				nonce[..8].copy_from_slice(&self.nonce.to_le_bytes());
				ChaCha8::new_from_slices(&self.key, &nonce)
					.unwrap()
					.apply_keystream(data)
			}
		}
	}

	/// Encrypt data in-place with the given key.
	///
	/// Returns the nonce and hash.
	fn encrypt(self, data: &mut [u8]) -> (u64, [u8; 16]) {
		match self.ty {
			CipherType::NoneXxh3 => {}
			CipherType::ChaCha8Poly1305 => {
				let mut nonce = [0; 12];
				nonce[..8].copy_from_slice(&self.nonce.to_le_bytes());
				ChaCha8::new_from_slices(&self.key, &nonce)
					.unwrap()
					.apply_keystream(data)
			}
		}
		(self.nonce, self.hash(data))
	}
}

impl HeaderCipher {
	/// Decrypt data in-place with the given key.
	pub(crate) fn decrypt(self, hash: &[u8; 16], data: &mut [u8]) -> Result<(), ()> {
		if ct_eq(*hash, self.cipher.hash(data)) {
			self.cipher.decrypt(data);
			Ok(())
		} else {
			Err(())
		}
	}

	/// Encrypt data in-place with the given key.
	///
	/// Returns the nonce and hash.
	pub(crate) fn encrypt(self, data: &mut [u8]) -> (u64, [u8; 16]) {
		self.cipher.encrypt(data)
	}
}

impl RecordCipher {
	/// Trim the data based on `real_len` and the cipher
	fn trim<'a>(&self, data: &'a mut [u8]) -> &'a mut [u8] {
		let real_len = usize::try_from(self.real_len).unwrap();
		match self.cipher.ty {
			CipherType::NoneXxh3 => &mut data[..real_len.next_multiple_of(64)],
			CipherType::ChaCha8Poly1305 => data,
		}
	}

	/// Decrypt data in-place with the given key.
	pub(crate) fn decrypt(self, hash: &[u8; 12], data: &mut [u8]) -> Result<(), ()> {
		let data = self.trim(data);
		let mut h1 = [0; 16];
		let mut h2 = self.cipher.hash(data);
		h1[..12].copy_from_slice(hash);
		h2[12..].fill(0);
		if ct_eq(h1, h2) {
			self.cipher.decrypt(data);
			Ok(())
		} else {
			Err(())
		}
	}

	/// Encrypt data in-place with the given key.
	///
	/// Returns the nonce and hash.
	pub(crate) fn encrypt(self, data: &mut [u8]) -> (u64, [u8; 12]) {
		let data = self.trim(data);
		let (nonce, [hash @ .., _, _, _, _]) = self.cipher.encrypt(data);
		(nonce, hash)
	}
}

fn ct_eq(lhs: [u8; 16], rhs: [u8; 16]) -> bool {
	u128::from_ne_bytes(lhs) == u128::from_ne_bytes(rhs)
}
