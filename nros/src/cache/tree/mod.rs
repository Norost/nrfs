pub mod data;
mod fetch;
mod get;
mod grow;
mod set;
mod shrink;
mod update_record;
mod write_zeros;

use {
	super::{Busy, Cache, EntryRef, Key, Present, RefCount, Slot, OBJECT_LIST_ID, RECORD_SIZE_P2},
	crate::{resource::Buf, Background, Dev, Error, MaxRecordSize, Record, Resource},
	core::ops::RangeInclusive,
};

/// Implementation of a record tree.
///
/// As long as a `Tree` object for a specific ID is alive its [`TreeData`] entry will not be
/// evicted.
#[derive(Clone, Debug)]
pub struct Tree<'a, 'b, D: Dev, R: Resource> {
	/// Underlying cache.
	cache: &'a Cache<D, R>,
	/// Background task runner.
	background: &'b Background<'a, D>,
	/// ID of the object.
	id: u64,
}

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
	/// Access a tree.
	pub(super) fn new(
		cache: &'a Cache<D, R>,
		bg: &'b Background<'a, D>,
		id: u64,
	) -> Tree<'a, 'b, D, R> {
		Self { cache, background: bg, id }
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

		let root_len = self.len().await?;

		// Ensure all data fits.
		let data = if offset >= root_len {
			return Ok(0);
		} else if offset.saturating_add(u64::try_from(data.len()).unwrap()) >= root_len {
			&data[..usize::try_from(root_len - offset).unwrap()]
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
			entry.modify(self.background, |b| {
				let min_len = last_offset.max(b.len());
				b.resize(min_len, 0);
				b.get_mut()[first_offset..last_offset].copy_from_slice(data);
			});
		} else {
			// We need to slice the first & last record once and operate on the others in full.
			let mut data = data;
			let mut range = range.into_iter();

			let first_key = range.next().unwrap();
			let last_key = range.next_back().unwrap();

			// Copy to first record |----xxxx|
			{
				let d;
				(d, data) = data.split_at((1usize << self.max_record_size()) - first_offset);

				let entry = self.get(0, first_key).await?;
				entry.modify(self.background, |b| {
					b.resize(first_offset, 0);
					b.extend_from_slice(d);
				});
			}

			// Copy middle records |xxxxxxxx|
			for offset in range {
				let d;
				(d, data) = data.split_at(1usize << self.max_record_size());

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
				entry.modify(self.background, |b| {
					let min_len = b.len().max(data.len());
					b.resize(min_len, 0);
					b.get_mut()[..last_offset].copy_from_slice(data);
				});
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

		let root_len = self.len().await?;

		// Ensure all data fits in buffer.
		let buf = if root_len <= offset {
			return Ok(0);
		} else if offset.saturating_add(u64::try_from(buf.len()).unwrap()) >= root_len {
			&mut buf[..usize::try_from(root_len - offset).unwrap()]
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
				(b, buf) = buf.split_at_mut((1usize << self.max_record_size()) - first_offset);
				let d = self.get(0, first_key).await?;
				copy(b, d.get().get(first_offset..).unwrap_or(&[]));
			}

			// Copy middle records |xxxxxxxx|
			for key in range {
				let b;
				(b, buf) = buf.split_at_mut(1usize << self.max_record_size());
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
		let (root, len) = self.root().await?;
		if new_len < len {
			self.shrink(new_len, &root).await
		} else if new_len > len {
			self.grow(new_len, &root).await
		} else {
			trace!(info "len is {}, nothing to do", len);
			Ok(())
		}
	}

	/// The length of the record tree in bytes.
	pub async fn len(&self) -> Result<u64, Error<D>> {
		trace!("len id {:#x}", self.id);
		self.root().await.map(|(_, len)| len)
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
	pub async fn replace_with(&self, other: Tree<'a, 'b, D, R>) -> Result<(), Error<D>> {
		self.cache
			.move_object(self.background, other.id, self.id)
			.await
	}

	/// Increase the reference count of an object.
	///
	/// This may fail if the reference count is already [`u16::MAX`].
	/// On failure, the returned value is `false`, otherwise `true`.
	pub async fn increase_reference_count(&self) -> Result<bool, Error<D>> {
		debug_assert_ne!(
			self.id, OBJECT_LIST_ID,
			"object list isn't reference counted"
		);

		let (mut obj, _) = self.cache.fetch_object(self.background, self.id).await?;
		let mut root = obj.data.root();
		debug_assert!(root.references != 0, "invalid object");
		if root.references == u16::MAX {
			return Ok(false);
		}
		root.references += 1;
		obj.data.set_root(&root);

		Ok(true)
	}

	/// Decrease the reference count of an object.
	///
	/// If the reference count reaches 0 the object is destroyed
	/// and the tree should not be used anymore.
	pub async fn decrease_reference_count(&self) -> Result<(), Error<D>> {
		trace!("decrease_reference_count {:#x}", self.id);
		debug_assert_ne!(
			self.id, OBJECT_LIST_ID,
			"object list isn't reference counted"
		);

		let (mut obj, _) = self.cache.fetch_object(self.background, self.id).await?;
		let mut root = obj.data.root();
		debug_assert!(root.references != 0, "invalid object");
		root.references -= 1;
		obj.data.set_root(&root);

		if root.references == 0 {
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

	/// Get the root and length of this tree.
	async fn root(&self) -> Result<(Record, u64), Error<D>> {
		let (obj, _) = self.cache.fetch_object(self.background, self.id).await?;
		#[cfg(debug_assertions)]
		obj.data.check_integrity();
		Ok((obj.data.root(), u64::from(obj.data.root().total_length)))
	}

	/// Get a reference to the background task runner.
	pub fn background_runner(&self) -> &'b Background<'a, D> {
		self.background
	}
}

/// Determine record range given an offset, record size and length.
///
/// Ranges are used for efficient iteration.
fn calc_range(record_size: MaxRecordSize, offset: u64, length: usize) -> RangeInclusive<u64> {
	let start_key = offset >> record_size;
	let end_key = (offset + u64::try_from(length).unwrap()) >> record_size;
	start_key..=end_key
}

/// Determine start & end offsets inside records.
fn calc_record_offsets(record_size: MaxRecordSize, offset: u64, length: usize) -> (usize, usize) {
	let mask = (1u64 << record_size) - 1;
	let start = offset & mask;
	let end = (offset + u64::try_from(length).unwrap()) & mask;
	(start.try_into().unwrap(), end.try_into().unwrap())
}

/// Calculate divmod with a power of two.
fn divmod_p2(offset: u64, pow2: u8) -> (u64, usize) {
	let mask = (1u64 << pow2) - 1;

	let index = offset & mask;
	let offt = offset >> pow2;

	(offt, index.try_into().unwrap())
}

/// Calculate depth given record size and total length.
pub(super) fn depth(max_record_size: MaxRecordSize, len: u64) -> u8 {
	if len == 0 {
		0
	} else {
		let mut depth = 0;
		// max len = maximum amount of bytes record tree can hold at current depth minus 1
		let mut max_len_mask = (1u64 << max_record_size) - 1;
		let len_mask = len - 1;
		while len_mask > max_len_mask {
			depth += 1;
			max_len_mask |= max_len_mask << max_record_size.to_raw() - RECORD_SIZE_P2;
		}
		depth + 1
	}
}

/// Calculate upper offset limit for cached entries for given depth and record size.
pub(super) fn max_offset(max_record_size: MaxRecordSize, depth: u8) -> u128 {
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
