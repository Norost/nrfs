use crate::resource::Buf;

pub fn max_output_size(len: usize) -> usize {
	len
}

pub fn compress(data: &[u8], buf: &mut [u8]) -> u32 {
	buf[..data.len()].copy_from_slice(data);
	data.len() as _
}

pub fn decompress<B: Buf>(data: &[u8], buf: &mut B, _max_size: usize) -> bool {
	buf.extend_from_slice(data);
	true
}
