use core::num::{NonZeroU32, NonZeroU8};

#[derive(Clone, Copy, Debug)]
pub enum KeyDerivation {
	None,
	Argon2id { p: NonZeroU8, t: NonZeroU32, m: NonZeroU32 },
}

impl KeyDerivation {
	/// Parse serialized key derivation parameters.
	pub fn from_raw(algorithm: u8, buf: &[u8; 8]) -> Option<Self> {
		Some(match algorithm {
			0 => Self::None,
			1 => {
				let &[t0, t1, t2, p, m0, m1, m2, m3] = buf;
				let t = u32::from_le_bytes([t0, t1, t2, 0]);
				let m = u32::from_le_bytes([m0, m1, m2, m3]);
				let p = NonZeroU8::new(p)?;
				let t = NonZeroU32::new(t)?;
				let m = NonZeroU32::new(m)?;
				Self::Argon2id { p, t, m }
			}
			_ => return None,
		})
	}

	/// Serialize key derivation parameters.
	pub fn to_raw(self) -> (u8, [u8; 8]) {
		let mut buf = [0; 8];
		match self {
			Self::None => (0, buf),
			Self::Argon2id { p, t, m } => {
				buf[0..3].copy_from_slice(&t.get().to_le_bytes());
				buf[3] = p.get();
				buf[4..8].copy_from_slice(&m.get().to_le_bytes());
				(1, buf)
			}
		}
	}
}

/// Derive key with argon2id.
///
/// Returns derived key and its hash.
pub fn argon2id(
	password: &[u8],
	uid: &[u8; 16],
	m: NonZeroU32,
	t: NonZeroU32,
	p: NonZeroU8,
) -> [u8; 32] {
	use argon2::{Algorithm, Argon2, Params, Version};
	let params = Params::new(m.get(), t.get(), p.get().into(), Some(32)).unwrap();
	let kdf = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
	let mut key = [0; 32];
	kdf.hash_password_into(password, uid, &mut key).unwrap();
	key
}
