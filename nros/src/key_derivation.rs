use core::num::{NonZeroU32, NonZeroU8};

#[derive(Clone, Copy, Debug)]
pub enum KeyDerivation {
	None,
	Argon2id { p: NonZeroU8, t: NonZeroU32, m: NonZeroU32 },
}

impl KeyDerivation {
	/// Parse serialized key derivation parameters.
	pub fn from_raw(buf: &[u8; 15]) -> Option<Self> {
		Some(match buf[0] {
			0 => Self::None,
			1 => {
				let f = |i| {
					let i = buf[i..i + 4].try_into().unwrap();
					let i = u32::from_le_bytes(i);
					NonZeroU32::new(i).unwrap()
				};
				Self::Argon2id { p: NonZeroU8::new(buf[1]).unwrap(), t: f(4), m: f(8) }
			}
			_ => return None,
		})
	}

	/// Serialize key derivation parameters.
	pub fn to_raw(self) -> [u8; 15] {
		let mut buf = [0; 15];
		match self {
			Self::None => {}
			Self::Argon2id { p, t, m } => {
				buf[0] = 1;
				buf[1] = p.get();
				buf[4..8].copy_from_slice(&t.get().to_le_bytes());
				buf[8..12].copy_from_slice(&m.get().to_le_bytes());
			}
		}
		buf
	}
}

/// Derive key as argon2id
///
/// Returns `None` if not set as a password derivation function.
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
	let mut buf = [0; 32];
	kdf.hash_password_into(password, uid, &mut buf).unwrap();
	buf
}
