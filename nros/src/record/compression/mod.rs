mod lz4;
mod none;

use crate::MaxRecordSize;

n2e! {
	[Compression]
	0 None
	1 Lz4
}

impl Compression {
	/// # Note
	///
	/// While [`Self::compress`] will always ensure that the compressed data is no larger than
	/// the uncompressed data, there still has to be some slack to ensure the compression
	/// algorithms do not run out of memory.
	pub(crate) fn max_output_size(self, len: usize, max_record_size: MaxRecordSize) -> usize {
		match self {
			Compression::None => none::max_output_size(len),
			Compression::Lz4 => lz4::max_output_size(len),
		}.min(1 << max_record_size.to_raw())
	}

	/// # Note
	///
	/// This may switch to another algorithm (e.g. `None`) to achieve smaller sizes and/or stay
	/// within record boundaries.
	pub(crate) fn compress(self, data: &[u8], buf: &mut [u8]) -> (Self, u32) {
		let res = match self {
			Compression::None => return (self, none::compress(data, buf)),
			Compression::Lz4 => lz4::compress(data, buf),
		};
		match res {
			Some(n) if n < data.len() => (self, n as _),
			// Compression made the data larger, so just don't compress.
			_ => (Compression::None, none::compress(data, buf)),
		}
	}

	pub(crate) fn decompress(self, data: &[u8], buf: &mut Vec<u8>, max_size: usize) -> bool {
		match self {
			Compression::None => none::decompress(data, buf, max_size),
			Compression::Lz4 => lz4::decompress(data, buf, max_size),
		}
	}
}
