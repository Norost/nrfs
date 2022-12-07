use super::*;

macro_rules! t {
	($mod:ident $comp:ident) => {
		mod $mod {
			use super::*;

			#[test]
			fn compress_zeros() {
				let data = [0; 256];
				let mut b = vec![0; data.len() * 2];
				let r = Record::pack(&data, &mut b, Compression::$comp);
				assert_eq!(
					r.compression,
					Compression::$comp as u8,
					concat!(stringify!($comp), " was not used")
				);
			}

			#[test]
			fn decompress_zeros() {
				let data = [0; 256];
				let mut b = vec![0; data.len() * 2];
				let r = Record::pack(&data, &mut b, Compression::$comp);
				let mut d = vec![0; data.len()];
				r.unpack(&b[..u32::from(r.length) as _], &mut d, MaxRecordSize::K1).unwrap();
				assert_eq!(&data, &*d);
			}
		}
	};
}

t!(none None);
t!(lz4 Lz4);
