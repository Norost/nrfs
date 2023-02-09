mod lz4;
mod none;

use {
	crate::{BlockSize, Resource},
	core::fmt,
};

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
	pub(crate) fn max_output_size(self, len: usize) -> usize {
		match self {
			Compression::None => none::max_output_size(len),
			Compression::Lz4 => lz4::max_output_size(len),
		}
	}

	/// # Note
	///
	/// This may switch to another algorithm (e.g. `None`) to achieve smaller sizes and/or stay
	/// within record boundaries.
	pub(crate) fn compress(
		self,
		data: &[u8],
		buf: &mut [u8],
		block_size: BlockSize,
	) -> (Self, u32) {
		if buf.len() <= 1 << block_size.to_raw() {
			// It isn't worth compressing this record as it'll take up a full block anyways.
			return (Self::None, none::compress(data, buf));
		}
		let res = match self {
			Self::None => return (self, none::compress(data, buf)),
			Self::Lz4 => lz4::compress(data, buf),
		};
		match res {
			Some(n) if n < data.len() => (self, n as _),
			// Compression made the data larger, so just don't compress.
			_ => (Self::None, none::compress(data, buf)),
		}
	}

	pub(crate) fn decompress<R: Resource>(
		self,
		data: &[u8],
		buf: &mut R::Buf,
		max_size: usize,
	) -> bool {
		match self {
			Compression::None => none::decompress::<R>(data, buf, max_size),
			Compression::Lz4 => lz4::decompress::<R>(data, buf, max_size),
		}
	}
}

impl Default for Compression {
	fn default() -> Self {
		Compression::Lz4
	}
}

impl fmt::Display for Compression {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::None => "none",
			Self::Lz4 => "lz4",
		}
		.fmt(f)
	}
}
