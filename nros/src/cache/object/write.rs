use {
	super::{Dev, Object, Resource},
	crate::{resource::Buf, Error},
};

impl<'a, D: Dev, R: Resource> Object<'a, D, R> {
	/// Write data to a range.
	///
	/// Returns the actual amount of bytes written.
	/// It may exit early if the necessary data is not cached (e.g. partial record write)
	pub async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, Error<D>> {
		trace!(
			"write id {:#x}, offset {}, len {}",
			self.id,
			offset,
			data.len()
		);

		let max_len = self.max_len();

		// Ensure all data fits.
		let data = if offset >= max_len {
			return Ok(0);
		} else if offset.saturating_add(u64::try_from(data.len()).unwrap()) >= max_len {
			&data[..usize::try_from(max_len - offset).unwrap()]
		} else {
			data
		};

		if data.is_empty() {
			return Ok(0);
		}

		let range = self.calc_range(offset, data.len());
		let (first_offset, last_offset) = self.calc_record_offsets(offset, data.len());

		if range.start() == range.end() {
			// We need to slice one record twice
			let entry = self.get(*range.start()).await?;
			entry.write(first_offset, data);
		} else {
			// We need to slice the first & last record once and operate on the others in full.
			let mut data = data;
			let mut range = range.into_iter();

			let first_key = (first_offset != 0).then(|| range.next().unwrap());
			let last_key = range.next_back().unwrap();

			// Copy to first record |----xxxx|
			// Don't bother if we can write out an entire record at once.
			if let Some(first_key) = first_key {
				let d;
				(d, data) = data.split_at((1 << self.cache.max_rec_size().to_raw()) - first_offset);

				let entry = self.get(first_key).await?;
				entry.write(first_offset, d);
			}

			// Copy middle records |xxxxxxxx|
			for offset in range {
				let d;
				(d, data) = data.split_at(1 << self.cache.max_rec_size().to_raw());

				let end = d.len() - d.iter().rev().position(|&b| b != 0).unwrap_or(d.len());
				let mut buf = self.cache.resource().alloc();
				buf.extend_from_slice(&d[..end]);

				self.set(offset, buf).await?;
			}

			// Copy end record |xxxx----|
			// Don't bother if there is no data
			if last_offset > 0 {
				debug_assert_eq!(data.len(), last_offset);
				let entry = self.get(last_key).await?;
				entry.write(0, data);
			}
		}

		Ok(data.len())
	}
}
