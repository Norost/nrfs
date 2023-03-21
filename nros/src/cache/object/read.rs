use {
	super::{Dev, Object, Resource},
	crate::Error,
};

impl<'a, D: Dev, R: Resource> Object<'a, D, R> {
	/// Read data from a range.
	///
	/// Returns the actual amount of bytes read.
	/// It may exit early if not all data is cached.
	pub async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
		trace!(
			"read id {:#x}, offset {}, len {}",
			self.id,
			offset,
			buf.len()
		);

		let max_len = self.max_len();

		// Ensure all data fits in buffer.
		let buf = if max_len <= offset {
			return Ok(0);
		} else if offset.saturating_add(u64::try_from(buf.len()).unwrap()) >= max_len {
			&mut buf[..usize::try_from(max_len - offset).unwrap()]
		} else {
			buf
		};

		if buf.is_empty() {
			return Ok(0);
		}

		let range = self.calc_range(offset, buf.len());
		let (first_offset, last_offset) = self.calc_record_offsets(offset, buf.len());

		let buf_len = buf.len();

		if range.start() == range.end() {
			// We need to slice one record twice
			self.get(*range.start()).await?.read(first_offset, buf);
		} else {
			// We need to slice the first & last record once and operate on the others in full.
			let mut buf = buf;
			let mut range = range.into_iter();

			let first_key = range.next().unwrap();
			let last_key = range.next_back().unwrap();

			// Copy to first record |----xxxx|
			{
				let b;
				(b, buf) =
					buf.split_at_mut((1 << self.cache.max_rec_size().to_raw()) - first_offset);
				self.get(first_key).await?.read(first_offset, b);
			}

			// Copy middle records |xxxxxxxx|
			for key in range {
				let b;
				(b, buf) = buf.split_at_mut(1 << self.cache.max_rec_size().to_raw());
				self.get(key).await?.read(0, b);
			}

			// Copy end record |xxxx----|
			// Don't bother if there's nothing to copy
			if last_offset > 0 {
				debug_assert_eq!(buf.len(), last_offset);
				self.get(last_key).await?.read(0, buf);
			}
		}

		Ok(buf_len)
	}
}
