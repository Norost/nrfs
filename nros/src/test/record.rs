use {
	super::*,
	crate::data::{cipher::Cipher, record},
};

macro_rules! t {
	($mod:ident $comp:ident $($cipher_mod:ident $cipher:ident)*) => {
		mod $mod {
			use super::*;

			$(mod $cipher_mod {
				use super::*;

				fn cipher() -> Cipher {
					Cipher {
						key1: [0xcc; 32],
						key2: [0xdd; 32],
						ty: CipherType::$cipher,
					}
				}

				#[test]
				fn compress_zeros() {
					let data = &[0; 1024];
					let b = &mut [0; 2048];
					let _len = record::pack(data, b, Compression::$comp, BlockSize::B512, cipher(), &[0; 24]);
					// FIXME
					/*
					assert_eq!(
						r.compression(),
						Ok(Compression::$comp),
						concat!(stringify!($comp), " was not used")
					);
					*/
				}

				#[test]
				fn decompress_zeros() {
					let data = &[0; 1024];
					let b = &mut [0; 2048];
					let blks = record::pack(data, b, Compression::$comp, BlockSize::B512, cipher(), &[0; 24]);
					let b = &mut b[..usize::from(blks) << BlockSize::B512.to_raw()];

					let res = StdResource::new();
					let d = record::unpack(b, res.alloc(), MaxRecordSize::K1, cipher()).unwrap();
					assert_eq!(data, &*d);
				}
			})*
		}
	};
}

t!(none None none_xxh3 NoneXxh3 xchacha12_poly1305 XChaCha12Poly1305);
t!(lz4 Lz4 none_xxh3 NoneXxh3 xchacha12_poly1305 XChaCha12Poly1305);
