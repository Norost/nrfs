use crate::resource::Buf;

pub fn max_output_size(len: usize) -> usize {
	lz4_flex::block::get_maximum_output_size(len)
}

pub fn compress(data: &[u8], buf: &mut [u8]) -> Option<usize> {
	lz4_flex::block::compress_into(data, buf).ok()
}

pub fn decompress<B: Buf>(data: &[u8], buf: &mut B, max_size: usize) -> bool {
	buf.resize(max_size, 0);
	if let Ok(l) = lz4_flex::block::decompress_into(data, buf.get_mut()) {
		buf.resize(l, 0);
		buf.shrink();
		true
	} else {
		false
	}
}
