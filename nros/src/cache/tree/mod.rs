pub mod data;
mod fetch;
mod get;
mod grow;
mod set;
mod shrink;
mod update_record;
mod view;
mod write_zeros;

use {
	super::{Busy, Cache, EntryRef, Key, Object, Present, Slot, OBJECT_LIST_ID, RECORD_SIZE_P2},
	crate::{resource::Buf, Dev, Error, MaxRecordSize, Resource},
	core::ops::RangeInclusive,
};

/// Implementation of a record tree.
///
/// As long as a `Tree` object for a specific ID is alive its [`TreeData`] entry will not be
/// evicted.
#[derive(Clone, Debug)]
pub struct Tree<'a, D: Dev, R: Resource> {
	/// Underlying cache.
	cache: &'a Cache<D, R>,
	/// ID of the object.
	id: u64,
}

impl<'a, D: Dev, R: Resource> Tree<'a, D, R> {
	/// Access a tree.
	pub(super) fn new(cache: &'a Cache<D, R>, id: u64) -> Tree<'a, D, R> {
		Self { cache, id }
	}

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

		let object_len = self.len().await?;

		// Ensure all data fits.
		let data = if offset >= object_len {
			return Ok(0);
		} else if offset.saturating_add(u64::try_from(data.len()).unwrap()) >= object_len {
			&data[..usize::try_from(object_len - offset).unwrap()]
		} else {
			data
		};

		if data.is_empty() {
			return Ok(0);
		}

		let range = calc_range(self.max_record_size(), offset, data.len());
		let (first_offset, last_offset) =
			calc_record_offsets(self.max_record_size(), offset, data.len());

		if range.start() == range.end() {
			// We need to slice one record twice
			let entry = self.get(0, *range.start()).await?;
			entry.write(first_offset, data).await;
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
				(d, data) = data.split_at((1 << self.max_record_size().to_raw()) - first_offset);

				let entry = self.get(0, first_key).await?;
				entry.write(first_offset, d).await;
			}

			// Copy middle records |xxxxxxxx|
			for offset in range {
				let d;
				(d, data) = data.split_at(1 << self.max_record_size().to_raw());

				let end = d.len() - d.iter().rev().position(|&b| b != 0).unwrap_or(d.len());
				let mut buf = self.cache.resource().alloc();
				buf.extend_from_slice(&d[..end]);

				self.set(offset, buf).await?;
			}

			// Copy end record |xxxx----|
			// Don't bother if there is no data
			if last_offset > 0 {
				debug_assert_eq!(data.len(), last_offset);
				let entry = self.get(0, last_key).await?;
				entry.write(0, data).await;
			}
		}

		Ok(data.len())
	}

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

		let object_len = self.len().await?;

		// Ensure all data fits in buffer.
		let buf = if object_len <= offset {
			return Ok(0);
		} else if offset.saturating_add(u64::try_from(buf.len()).unwrap()) >= object_len {
			&mut buf[..usize::try_from(object_len - offset).unwrap()]
		} else {
			buf
		};

		if buf.is_empty() {
			return Ok(0);
		}

		let range = calc_range(self.max_record_size(), offset, buf.len());
		let (first_offset, last_offset) =
			calc_record_offsets(self.max_record_size(), offset, buf.len());

		/// Copy from record to first half of `buf` and fill remainder with zeroes.
		#[track_caller]
		fn copy(buf: &mut [u8], data: &[u8]) {
			buf[..data.len()].copy_from_slice(data);
			buf[data.len()..].fill(0);
		}

		let buf_len = buf.len();

		if range.start() == range.end() {
			// We need to slice one record twice
			let b = self.get(0, *range.start()).await?;

			let b = b.get().get(first_offset..).unwrap_or(&[]);
			copy(buf, &b[..buf.len().min(b.len())]);
		} else {
			// We need to slice the first & last record once and operate on the others in full.
			let mut buf = buf;
			let mut range = range.into_iter();

			let first_key = range.next().unwrap();
			let last_key = range.next_back().unwrap();

			// Copy to first record |----xxxx|
			{
				let b;
				(b, buf) = buf.split_at_mut((1 << self.max_record_size().to_raw()) - first_offset);
				let d = self.get(0, first_key).await?;
				copy(b, d.get().get(first_offset..).unwrap_or(&[]));
			}

			// Copy middle records |xxxxxxxx|
			for key in range {
				let b;
				(b, buf) = buf.split_at_mut(1 << self.max_record_size().to_raw());
				let d = self.get(0, key).await?;
				copy(b, d.get());
			}

			// Copy end record |xxxx----|
			// Don't bother if there's nothing to copy
			if last_offset > 0 {
				debug_assert_eq!(buf.len(), last_offset);
				let d = self.get(0, last_key).await?;
				let max_len = d.len().min(buf.len());
				copy(buf, &d.get()[..max_len]);
			}
		}

		Ok(buf_len)
	}

	/// Resize record tree.
	pub async fn resize(&self, new_len: u64) -> Result<(), Error<D>> {
		trace!("resize id {:#x} new_len {}", self.id, new_len);
		let (object, len) = self.object().await?;
		if new_len < len {
			self.shrink(new_len, &object).await
		} else if new_len > len {
			self.grow(new_len, &object).await
		} else {
			trace!(info "len is {}, nothing to do", len);
			Ok(())
		}
	}

	/// The length of the record tree in bytes.
	pub async fn len(&self) -> Result<u64, Error<D>> {
		trace!("len id {:#x}", self.id);
		self.object().await.map(|(_, len)| len)
	}

	/// Replace the data of this object with the data of another object.
	///
	/// The other object is destroyed.
	///
	/// # Panics
	///
	/// There is more than one active lock on the other object,
	/// i.e. there are multiple [`Tree`] instances referring to the same object.
	/// Hence the object cannot safely be destroyed.
	pub async fn replace_with(&self, other: Tree<'a, D, R>) -> Result<(), Error<D>> {
		self.cache.move_object(other.id, self.id).await
	}

	/// Increase the reference count of an object.
	///
	/// This may fail if the reference count is already [`u64::MAX`].
	/// On failure, the returned value is `false`, otherwise `true`.
	pub async fn increase_reference_count(&self) -> Result<bool, Error<D>> {
		debug_assert!(
			self.id & OBJECT_LIST_ID == 0,
			"object list & bitmap aren't reference counted"
		);

		let (mut obj, _) = self.cache.fetch_object(self.id).await?;
		let mut object = obj.data.object();
		debug_assert!(object.reference_count != 0, "invalid object");
		if object.reference_count == u64::MAX {
			return Ok(false);
		}
		object.reference_count += 1;
		obj.data.set_object(&object);

		Ok(true)
	}

	/// Decrease the reference count of an object.
	///
	/// If the reference count reaches 0 the object is destroyed
	/// and the tree should not be used anymore.
	pub async fn decrease_reference_count(&self) -> Result<(), Error<D>> {
		trace!("decrease_reference_count {:#x}", self.id);
		debug_assert!(
			self.id & OBJECT_LIST_ID == 0,
			"object list & bitmap aren't reference counted"
		);

		let (mut obj, _) = self.cache.fetch_object(self.id).await?;
		let mut object = obj.data.object();
		debug_assert!(object.reference_count != 0, "invalid object");
		object.reference_count -= 1;
		obj.data.set_object(&object);

		if object.reference_count == 0 {
			drop(obj);
			// Free space.
			self.resize(0).await?;
			self.cache.data.borrow_mut().dealloc_id(self.id);
		}

		Ok(())
	}

	/// Get the maximum record size.
	fn max_record_size(&self) -> MaxRecordSize {
		self.cache.max_record_size()
	}

	/// Get the ID of this object.
	pub fn id(&self) -> u64 {
		self.id
	}

	/// Get the object field of this tree.
	async fn object(&self) -> Result<(Object, u64), Error<D>> {
		let (obj, _) = self.cache.fetch_object(self.id).await?;
		#[cfg(debug_assertions)]
		obj.check_integrity();
		Ok((obj.data.object(), u64::from(obj.data.object().total_length)))
	}
}

/// Determine record range given an offset, record size and length.
///
/// Ranges are used for efficient iteration.
fn calc_range(record_size: MaxRecordSize, offset: u64, length: usize) -> RangeInclusive<u64> {
	let start_key = offset >> record_size.to_raw();
	let end_key = (offset + u64::try_from(length).unwrap()) >> record_size.to_raw();
	start_key..=end_key
}

/// Determine start & end offsets inside records.
fn calc_record_offsets(record_size: MaxRecordSize, offset: u64, length: usize) -> (usize, usize) {
	let mask = (1 << record_size.to_raw()) - 1;
	let start = offset & mask;
	let end = (offset + u64::try_from(length).unwrap()) & mask;
	(start.try_into().unwrap(), end.try_into().unwrap())
}

/// Calculate depth given record size and total length.
pub(super) fn depth(max_record_size: MaxRecordSize, len: u64) -> u8 {
	let Some(mut end) = len.checked_sub(1) else { return 0 };

	end >>= max_record_size.to_raw();

	let entries_per_rec_p2 = max_record_size.to_raw() - RECORD_SIZE_P2;

	let mut depth = 1;
	while end > 0 {
		depth += 1;
		end >>= entries_per_rec_p2
	}
	depth
}

/// Calculate upper offset limit for cached entries for given depth and record size.
///
/// The offset is *exclusive*, i.e. `[0; max_offset)`.
pub(super) fn max_offset(max_record_size: MaxRecordSize, depth: u8) -> u64 {
	if depth == 0 {
		0
	} else {
		1 << (max_record_size.to_raw() - RECORD_SIZE_P2) * (depth - 1)
	}
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn depth_rec1k_len0() {
		assert_eq!(depth(MaxRecordSize::K1, 0), 0);
	}

	#[test]
	fn depth_rec1k_len1() {
		assert_eq!(depth(MaxRecordSize::K1, 1), 1);
	}

	#[test]
	fn depth_rec1k_len1023() {
		assert_eq!(depth(MaxRecordSize::K1, 1023), 1);
	}

	#[test]
	fn depth_rec1k_len1024() {
		assert_eq!(depth(MaxRecordSize::K1, 1024), 1);
	}

	#[test]
	fn depth_rec1k_len1025() {
		assert_eq!(depth(MaxRecordSize::K1, 1025), 2);
	}

	#[test]
	fn depth_rec1k_2p10() {
		assert_eq!(depth(MaxRecordSize::K1, 1 << 10 + 5 * 0), 1);
	}

	#[test]
	fn depth_rec1k_2p15() {
		assert_eq!(depth(MaxRecordSize::K1, 1 << 10 + 5 * 1), 2);
	}

	#[test]
	fn depth_rec1k_2p20() {
		assert_eq!(depth(MaxRecordSize::K1, 1 << 10 + 5 * 2), 3);
	}

	#[test]
	fn depth_rec1k_2p20_plus_1() {
		assert_eq!(depth(MaxRecordSize::K1, (1 << 10 + 5 * 2) + 1), 4);
	}

	#[test]
	fn depth_rec1k_2p40() {
		assert_eq!(depth(MaxRecordSize::K1, 1 << 10 + 5 * 6), 7);
	}

	#[test]
	fn depth_rec1k_lenmax() {
		assert_eq!(depth(MaxRecordSize::K1, u64::MAX), 12);
	}
}
