use super::*;

#[test]
fn compress_trivial() {
	let mut b = vec![0; lz4_flex::block::get_maximum_output_size(256)];
	let r = Record::pack(&[0; 256], &mut b, Compression::Lz4);
	assert_eq!(r.compression, 1, "LZ4 was not used");
}
