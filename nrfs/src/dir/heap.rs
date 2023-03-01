use {
	super::{Dir, Offset},
	crate::{Dev, Error},
	core::cell::RefMut,
	rangemap::RangeSet,
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Read a heap value.
	pub(crate) async fn read_heap(&self, offset: Offset, buf: &mut [u8]) -> Result<(), Error<D>> {
		trace!("read_heap {:?} (len: {})", offset, buf.len());
		self.fs.get(self.id).await?.read(offset.into(), buf).await?;
		Ok(())
	}

	/// Write a heap value.
	pub(crate) async fn write_heap(&self, offset: Offset, data: &[u8]) -> Result<(), Error<D>> {
		trace!("write_heap {:?} (len: {})", offset, data.len());
		self.fs
			.get(self.id)
			.await?
			.write(offset.into(), data)
			.await?;
		Ok(())
	}

	/// Zero out heap region.
	pub(crate) async fn zero_heap(&self, offset: Offset, len: u64) -> Result<(), Error<D>> {
		trace!("zero_heap {:?} (len: {})", offset, len);
		self.fs
			.get(self.id)
			.await?
			.write_zeros(offset.into(), len)
			.await?;
		Ok(())
	}

	/// Write a full, minimized heap allocation log.
	pub(super) async fn save_heap_alloc_log(&self) -> Result<(), Error<D>> {
		trace!("save_heap_alloc_log");
		todo!();
	}

	/// Get or load the heap allocation map.
	pub(super) async fn heap_alloc_log(&self) -> Result<RefMut<'a, RangeSet<Offset>>, Error<D>> {
		trace!("heap_alloc_log");
		todo!();
	}

	/// Allocate heap space for arbitrary data.
	///
	/// The returned region is not readable until it is written to.
	pub(crate) async fn alloc_heap(&self, len: u64) -> Result<Offset, Error<D>> {
		trace!("alloc_heap {:?}", len);
		if len == 0 {
			return Ok(Offset::MIN);
		}
		let mut log = self.heap_alloc_log().await?;
		for r in log.gaps(&(Offset::MIN..Offset::MAX)) {
			if u64::from(r.end) - u64::from(r.start) >= len {
				log.insert(r.start..r.start.add_u64(len).unwrap());
				drop(log);
				self.save_heap_alloc_log().await?;
				return Ok(r.start);
			}
		}
		todo!("all 2^48 bytes are allocated");
	}

	/// Deallocate heap space.
	pub(crate) async fn dealloc_heap(&self, offset: Offset, len: u64) -> Result<(), Error<D>> {
		trace!("dealloc_heap {:?}", len);
		if len > 0 {
			// Write zeroes for compression and to make sure resized files don't include
			// garbage (non-zeroes).
			self.zero_heap(offset, len).await?;

			// Free region.
			// Happens after zeroing to avoid concurrent read/writes.
			let r = offset..offset.add_u64(len).unwrap();
			let mut log = self.heap_alloc_log().await?;
			debug_assert!(
				log.iter().any(|d| {
					let d = u64::from(d.start)..u64::from(d.end);
					let r = u64::from(r.start)..u64::from(r.end);
					r.into_iter().all(|e| d.contains(&e))
				}),
				"double free"
			);
			log.remove(r);
			drop(log);
			self.save_heap_alloc_log().await?;
		}
		Ok(())
	}
}

fn offt([a, b, c, d, e, f]: [u8; 6]) -> u64 {
	u64::from_le_bytes([a, b, c, d, e, f, 0, 0])
}
