mod data;
mod get;
mod key;
mod read;
mod set;
mod write;
mod write_zeros;

pub(super) use {
	data::ObjectData,
	key::{Key, RootIndex},
};

use {
	super::{
		memory_tracker::ENTRY_COST, Buf, Cache, Dev, Error, IdKey, Resource, Tree,
		OBJECT_BITMAP_ID, OBJECT_LIST_ID,
	},
	crate::{
		data::record::{Depth, RecordRef},
		util,
	},
	core::ops::RangeInclusive,
};

/// Reference to an object.
#[derive(Clone, Debug)]
pub struct Object<'a, D: Dev, R: Resource> {
	/// Underlying cache.
	cache: &'a Cache<D, R>,
	/// ID of the object.
	id: u64,
}

impl<'a, D: Dev, R: Resource> Object<'a, D, R> {
	/// Create a reference to an object.
	pub(super) fn new(cache: &'a Cache<D, R>, id: u64) -> Self {
		Self { cache, id }
	}

	/// Get the ID of this object.
	pub fn id(&self) -> u64 {
		self.id
	}

	/// Deallocate this object.
	///
	/// This zeros out all data in this object.
	pub async fn dealloc(&self) -> Result<(), Error<D>> {
		trace!("dealloc {:#x}", self.id);
		self.write_zeros(0, u64::MAX).await?;
		self.cache.object_set_allocated(self.id, false).await?;
		self.cache.data().dealloc_id(self.id);
		Ok(())
	}

	/// Determine start & end offsets inside records.
	fn calc_record_offsets(&self, offset: u64, length: usize) -> (usize, usize) {
		let mask = (1 << self.cache.max_rec_size().to_raw()) - 1;
		let start = offset & mask;
		let end = (offset + u64::try_from(length).unwrap()) & mask;
		(start.try_into().unwrap(), end.try_into().unwrap())
	}

	/// Determine record range given an offset, record size and length.
	///
	/// Ranges are used for efficient iteration.
	fn calc_range(&self, offset: u64, length: usize) -> RangeInclusive<u64> {
		let start_key = offset >> self.cache.max_rec_size().to_raw();
		let end_key =
			(offset + u64::try_from(length).unwrap()) >> self.cache.max_rec_size().to_raw();
		start_key..=end_key
	}

	/// Determine the tree containing the given leaf offset in object, along with offset in tree.
	///
	/// `None` if out of range.
	fn offset_to_tree(&self, offset: u64) -> Option<(RootIndex, u64)> {
		let mut sum = 0;
		for (i, size) in (RootIndex::I0..=RootIndex::I3).zip(self.cache.root_max_size) {
			let size = size >> self.cache.max_rec_size().to_raw();
			if offset < sum + size {
				return Some((i, offset - sum));
			}
			sum += size;
		}
		None
	}

	fn max_len(&self) -> u64 {
		self.cache
			.root_max_size
			.iter()
			.fold(0, |s, &x| s.saturating_add(x))
	}
}

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Grow the object list, i.e. add one level.
	pub(super) async fn grow_object_list<'a>(&'a self) -> Result<(), Error<D>> {
		trace!("grow_object_list");
		// Steps:
		// * take the top-level record.
		// * put root in new record.
		// * zero the original root.
		// Repeat for bitmap, if necessary.

		// FIXME add lock to avoid concurrent grow.

		let cur_list_depth = self.store.object_list_depth();
		let new_list_depth = cur_list_depth.next();

		// Check if depth of bitmap also needs to increase.
		let cur_bitmap_depth = self.object_bitmap_depth.get();
		let new_bitmap_depth = self.calc_bitmap_depth(new_list_depth);

		// Reserve memory.
		let count = 1 + usize::from(cur_bitmap_depth != new_bitmap_depth);
		let mut resv = self.memory_reserve((ENTRY_COST + 8) * count).await;

		// Fixup list and bitmap depth.
		let mut add_entry = |id, root: RecordRef, depth| {
			let mut buf = self.resource().alloc();
			let root = util::slice_trim_zeros_end(root.as_ref());
			buf.extend_from_slice(root);

			let r = resv.split(ENTRY_COST + buf.len());

			let key = Key::new(RootIndex::I0, depth, 0);
			let mut entry = self.entry_insert(IdKey { id, key }, buf, 0, r);
			entry.dirty_records.insert(key);
		};

		// List
		self.store.set_object_list_depth(new_list_depth);
		add_entry(
			OBJECT_LIST_ID,
			self.store.object_list_root(),
			new_list_depth,
		);
		self.store.set_object_list_root(RecordRef::NONE);

		// Bitmap
		if cur_bitmap_depth != new_bitmap_depth {
			add_entry(
				OBJECT_BITMAP_ID,
				self.store.object_bitmap_root(),
				new_bitmap_depth,
			);
			self.store.set_object_bitmap_root(RecordRef::NONE);
		}

		self.store.set_object_list_depth(new_list_depth);
		self.object_bitmap_depth.set(new_bitmap_depth);

		#[cfg(test)]
		self.verify_cache_usage();

		Ok(())
	}

	/// Determine the depth of the bitmap for the given depth of the object list.
	pub(super) fn calc_bitmap_depth(&self, obj_list_depth: Depth) -> Depth {
		// Determine highest valid *byte* offset of object bitmap.
		let offt = 1u64 << self.entries_per_parent_p2() * (obj_list_depth as u8);
		let offt = offt << self.max_rec_size().to_raw();
		let offt = offt / (32 * 8); // 32 byte objects + 1 bit per object
		let offt = offt - 1;

		// Determine depth from byte offset
		let mut depth = Depth::D0;
		let mut offt = offt >> self.max_rec_size().to_raw();
		while offt > 0 {
			depth = depth.next();
			offt >>= self.entries_per_parent_p2();
		}
		depth
	}

	/// Set whether an object is allocated.
	pub(super) async fn object_set_allocated(&self, id: u64, value: bool) -> Result<(), Error<D>> {
		trace!("object_set_allocated {:#x} {}", id, value);

		let (offt, bit) = util::divmod_p2(id, 3);
		let (offt, index) = util::divmod_p2(offt, self.max_rec_size().to_raw());
		let entry = Tree::object_bitmap(self).get(Depth::D0, offt).await?;

		let mut b = *entry.get().get(index).unwrap_or(&0);
		b &= !(1 << bit);
		b |= u8::from(value) << bit;
		entry.write(index, &[b]);
		Ok(())
	}
}
