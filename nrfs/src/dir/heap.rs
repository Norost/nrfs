use {
	super::{header::DirHeader, Dir},
	crate::{Dev, Error, Nrfs},
};

pub(crate) struct Heap<'a, D: Dev> {
	fs: &'a Nrfs<D>,
	id: u64,
}

impl<'a, D: Dev> Heap<'a, D> {
	/// Read a heap value.
	pub(crate) async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<(), Error<D>> {
		trace!("read_heap {:?} (len: {})", offset, buf.len());
		self.fs.get(self.id).read(offset.into(), buf).await?;
		Ok(())
	}

	/// Write a heap value.
	pub(crate) async fn write(&self, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		trace!("write_heap {:?} (len: {})", offset, data.len());
		self.fs.get(self.id).write(offset.into(), data).await?;
		Ok(())
	}

	/// Zero out heap region.
	pub(crate) async fn zero(&self, offset: u64, len: u64) -> Result<(), Error<D>> {
		trace!("zero_heap {:?} (len: {})", offset, len);
		self.fs.get(self.id).write_zeros(offset.into(), len).await?;
		Ok(())
	}

	/// Allocate heap space for arbitrary data.
	///
	/// The returned region is not readable until it is written to.
	pub(crate) async fn alloc(&self, hdr: &mut DirHeader<'_>, len: u64) -> Result<u64, Error<D>> {
		trace!("alloc_heap {:?}", len);
		if len == 0 {
			return Ok(0);
		}
		let addr = hdr.heap_length;
		hdr.heap_length += len;
		hdr.heap_allocated += len;
		Ok(addr)
	}

	/// Deallocate heap space.
	pub(crate) async fn dealloc(
		&self,
		hdr: &mut DirHeader<'_>,
		offset: u64,
		len: u64,
	) -> Result<(), Error<D>> {
		trace!("dealloc_heap {:?}", len);
		self.zero(offset, len).await?;
		hdr.heap_allocated -= len;
		if offset + len == hdr.heap_length {
			hdr.heap_length = offset;
		}
		Ok(())
	}
}

impl<'a, D: Dev> Dir<'a, D> {
	/// Get heap object.
	pub(crate) fn heap(&self, hdr: &DirHeader<'a>) -> Heap<'a, D> {
		Heap { fs: self.fs, id: hdr.heap_id }
	}
}
