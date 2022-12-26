use siphasher::sip::SipHasher13;

/// Hasher helper structure.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(any(test, fuzzing), derive(arbitrary::Arbitrary))]
pub enum Hasher {
	SipHasher13([u8; 16]),
}

impl Hasher {
	/// Turn this hasher into raw components for storage.
	///
	/// The first element represents the type,
	/// the second element represents the key.
	pub fn to_raw(self) -> (u8, [u8; 16]) {
		match self {
			Self::SipHasher13(h) => (1, h),
		}
	}

	/// Create a hasher from raw components.
	///
	/// Fails if the hasher type is unknown.
	pub fn from_raw(ty: u8, key: &[u8; 16]) -> Option<Self> {
		Some(match ty {
			1 => Self::SipHasher13(*key),
			_ => return None,
		})
	}

	/// Hash an arbitrary-sized key.
	pub fn hash(&self, data: &[u8]) -> u64 {
		use core::hash::Hasher;
		match self {
			Self::SipHasher13(key) => {
				let mut h = SipHasher13::new_with_key(key);
				h.write(data);
				h.finish()
			}
		}
	}
}
