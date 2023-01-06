use crate::{Resource, resource::Buf};

pub fn max_output_size(len: usize) -> usize {
	len
}

pub fn compress(data: &[u8], buf: &mut [u8]) -> u32 {
	buf[..data.len()].copy_from_slice(data);
	data.len() as _
}

pub fn decompress<R: Resource>(data: &[u8], buf: &mut R::Buf, _max_size: usize) -> bool {
	buf.extend_from_slice(data);
	true
}
