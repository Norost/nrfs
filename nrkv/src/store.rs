#[cfg(not(feature = "alloc"))]
compile_error!("Store currently requires alloc::Box");

use {
	alloc::{boxed::Box, vec::Vec},
	core::{
		future::{self, Future},
		pin::Pin,
	},
};

pub trait Store {
	type Error;

	fn read<'a>(
		&'a mut self,
		offset: u64,
		buf: &'a mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
	fn write<'a>(
		&'a mut self,
		offset: u64,
		data: &'a [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
	fn write_zeros<'a>(
		&'a mut self,
		offset: u64,
		len: u64,
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>>;
	fn len(&self) -> u64;
}

impl Store for [u8] {
	type Error = !;

	fn read(
		&mut self,
		offset: u64,
		buf: &mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + '_>> {
		let b = offset
			.try_into()
			.ok()
			.and_then(|o| self.get(o..o + <[u8]>::len(buf)))
			.expect("out of bounds");
		buf.copy_from_slice(b);
		Box::pin(future::ready(Ok(())))
	}

	fn write(
		&mut self,
		offset: u64,
		data: &[u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + '_>> {
		offset
			.try_into()
			.ok()
			.and_then(|o| self.get_mut(o..o + data.len()))
			.expect("out of bounds")
			.copy_from_slice(data);
		Box::pin(future::ready(Ok(())))
	}

	fn write_zeros(
		&mut self,
		offset: u64,
		len: u64,
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + '_>> {
		let f = |n: u64| n.try_into().ok();
		f(offset)
			.and_then(|s| offset.checked_add(len).and_then(f).map(|e| s..e))
			.and_then(|r| self.get_mut(r))
			.expect("out of bounds")
			.fill(0);
		Box::pin(future::ready(Ok(())))
	}

	fn len(&self) -> u64 {
		<[u8]>::len(self).try_into().unwrap_or(u64::MAX)
	}
}

macro_rules! store_slice {
	($ty:ty) => {
		impl Store for $ty {
			type Error = !;

			fn read<'a>(
				&'a mut self,
				offset: u64,
				buf: &'a mut [u8],
			) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>> {
				<[u8] as Store>::read(self, offset, buf)
			}
			fn write<'a>(
				&'a mut self,
				offset: u64,
				data: &'a [u8],
			) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>> {
				<[u8] as Store>::write(self, offset, data)
			}
			fn write_zeros<'a>(
				&'a mut self,
				offset: u64,
				len: u64,
			) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>> {
				<[u8] as Store>::write_zeros(self, offset, len)
			}
			fn len(&self) -> u64 {
				<[u8] as Store>::len(self)
			}
		}
	};
}
store_slice!(&mut [u8]);
#[cfg(feature = "alloc")]
store_slice!(Box<[u8]>);
#[cfg(feature = "alloc")]
store_slice!(Vec<u8>);
