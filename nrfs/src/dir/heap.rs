use {
	super::{Dir, HEAP_OFFT},
	crate::{Dev, Error},
	core::cell::RefMut,
	rangemap::RangeSet,
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Read a heap value.
	pub(crate) async fn read_heap(&self, offset: u64, buf: &mut [u8]) -> Result<(), Error<D>> {
		trace!("read_heap {:?} (len: {})", offset, buf.len());
		let obj = self.fs.storage.get(self.id + HEAP_OFFT).await?;
		crate::read_exact(&obj, offset, buf).await
	}

	/// Write a heap value.
	pub(crate) async fn write_heap(&self, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		trace!("write_heap {:?} (len: {})", offset, data.len());
		let obj = self.fs.storage.get(self.id + HEAP_OFFT).await?;
		crate::write_all(&obj, offset, data).await
	}

	/// Zero out heap region.
	pub(crate) async fn zero_heap(&self, offset: u64, len: u64) -> Result<(), Error<D>> {
		trace!("zero_heap {:?} (len: {})", offset, len);
		// TODO make use of write_zeros
		// We need to properly test the former first though.
		let obj = self.fs.storage.get(self.id + HEAP_OFFT).await?;
		let len = usize::try_from(len).unwrap();
		crate::write_all(&obj, offset, &vec![0; len]).await
	}

	/// Write a full, minimized heap allocation log.
	pub(super) async fn save_heap_alloc_log(&self) -> Result<(), Error<D>> {
		trace!("save_heap_alloc_log");
		let map = self.hashmap().await?;
		let log = self.heap_alloc_log().await?.clone();
		map.save_heap_alloc_log(&log).await
	}

	/// Get or load the heap allocation map.
	pub(super) async fn heap_alloc_log(&self) -> Result<RefMut<'a, RangeSet<u64>>, Error<D>> {
		trace!("heap_alloc_log");
		let map = self.hashmap().await?;
		map.heap_alloc_log().await
	}

	/// Allocate heap space for arbitrary data.
	///
	/// The returned region is not readable until it is written to.
	pub(crate) async fn alloc_heap(&self, len: u64) -> Result<u64, Error<D>> {
		trace!("alloc_heap {:?}", len);
		if len == 0 {
			return Ok(0);
		}
		let mut log = self.heap_alloc_log().await?;
		for r in log.gaps(&(0..u64::MAX)) {
			if r.end - r.start >= len {
				log.insert(r.start..r.start + len);
				let end = log.iter().last().map_or(0, |r| r.end);
				drop(log);

				// Resize heap
				let heap = self.fs.storage.get(self.id + HEAP_OFFT).await?;
				let len = heap.len().await?;
				heap.resize(len.max(end)).await?;
				drop(heap);

				// Save alloc log
				self.save_heap_alloc_log().await?;
				return Ok(r.start);
			}
		}
		// This is unreachable in practice.
		unreachable!("all 2^64 bytes are allocated");
	}

	/// Deallocate heap space.
	pub(crate) async fn dealloc_heap(&self, offset: u64, len: u64) -> Result<(), Error<D>> {
		trace!("dealloc_heap {:?}", len);
		if len > 0 {
			// Write zeroes for compression and to make sure resized files don't include
			// garbage (non-zeroes).
			self.zero_heap(offset, len).await?;

			// Free region.
			// Happens after zeroing to avoid concurrent read/writes.
			let r = offset..offset + len;
			let mut log = self.heap_alloc_log().await?;
			debug_assert!(
				log.iter().any(|d| r.clone().all(|e| d.contains(&e))),
				"double free"
			);
			log.remove(r);
			drop(log);
			self.save_heap_alloc_log().await?;
		}
		Ok(())
	}
}
