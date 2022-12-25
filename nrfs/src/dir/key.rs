use {
	super::Hasher,
	crate::Name,
	core::{fmt, num::NonZeroU8},
};

/// Entry key, which may be embedded or on the heap.
#[derive(Clone)]
pub(super) enum Key {
	Embed {
		/// The length of the key.
		///
		/// Must be `<= 27`.
		len: NonZeroU8,
		/// The data of the key.
		data: [u8; 27],
	},
	Heap {
		/// The length of the key.
		///
		/// Must be `> 27`.
		len: NonZeroU8,
		/// The offset of the key on the heap.
		offset: u64,
		/// The hash of the key.
		///
		/// Used to avoid heap fetches when moving or comparing entries.
		hash: u64,
	},
}

impl Key {
	/// Convert key to raw data.
	pub(super) fn to_raw(&self) -> [u8; 28] {
		let mut buf = [0; 28];
		match self {
			Self::Embed { len, data } => {
				buf[0] = len.get();
				buf[1..].copy_from_slice(data);
			}
			Self::Heap { len, offset, hash } => {
				buf[0] = len.get();
				buf[8..16].copy_from_slice(&offset.to_le_bytes());
				buf[16..24].copy_from_slice(&hash.to_le_bytes());
			}
		}
		buf
	}

	/// Create key from raw data.
	///
	/// Returns `None` if length is 0.
	pub(super) fn from_raw(data: &[u8; 28]) -> Option<Self> {
		let &[len, data @ ..] = data;
		let len = NonZeroU8::new(len)?;
		Some(if len.get() <= 27 {
			Self::Embed { len, data }
		} else {
			let offset = u64::from_le_bytes(data[7..15].try_into().unwrap());
			let hash = u64::from_le_bytes(data[15..23].try_into().unwrap());
			Self::Heap { len, offset, hash }
		})
	}

	/// Take or calculate the hash from the existing key.
	pub(super) fn hash(&self, hasher: &Hasher) -> u64 {
		match self {
			Self::Embed { len, data: d } => hasher.hash(&d[..len.get().into()]),
			Self::Heap { len: _, offset: _, hash } => *hash,
		}
	}
}

impl fmt::Debug for Key {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			&Self::Embed { len, data } => f
				.debug_struct(stringify!(Embed))
				.field("len", &len)
				.field(
					"data",
					&<&Name>::try_from(&data[..len.get().into()]).unwrap(),
				)
				.finish(),
			&Self::Heap { len, offset, hash } => f
				.debug_struct(stringify!(Heap))
				.field("len", &len)
				.field("offset", &offset)
				.field("hash", &format_args!("{:#018x}", &hash))
				.finish(),
		}
	}
}
