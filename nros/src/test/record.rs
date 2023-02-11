use super::*;

macro_rules! t {
	($mod:ident $comp:ident $($cipher_mod:ident $cipher:ident)*) => {
		mod $mod {
			use super::*;

			$(mod $cipher_mod {
				use super::*;

				fn cipher() -> Cipher {
					Cipher {
						key: [0xcc; 32],
						nonce: 0xdedede,
						ty: CipherType::$cipher,
					}
				}

				#[test]
				fn compress_zeros() {
					let data = &[0; 1024];
					let b = &mut [0; 2048];
					let r = Record::pack(data, b, Compression::$comp, BlockSize::B512, cipher());
					assert_eq!(
						r.compression(),
						Ok(Compression::$comp),
						concat!(stringify!($comp), " was not used")
					);
				}

				#[test]
				fn decompress_zeros() {
					let data = &[0; 1024];
					let b = &mut [0; 2048];
					let r = Record::pack(data, b, Compression::$comp, BlockSize::B512, cipher());
					let b = &mut b[..BlockSize::B512.round_up(r.length())];

					let res = StdResource::new();
					let d = res.alloc();
					let d = r.unpack::<StdResource>(b, d, MaxRecordSize::K1, cipher()).unwrap();
					assert_eq!(data, &*d);
				}
			})*
		}
	};
}

t!(none None none_xxh3 NoneXxh3 chacha8_poly1305 ChaCha8Poly1305);
t!(lz4 Lz4 none_xxh3 NoneXxh3 chacha8_poly1305 ChaCha8Poly1305);
