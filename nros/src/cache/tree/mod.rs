mod fetch;
mod get;
mod set;
mod set_zeros;
mod update_record;

use {
	super::{
		util, Cache, Depth, EntryRef, IdKey, Key, RootIndex, OBJECT_SIZE_P2, RECORDREF_SIZE_P2,
	},
	crate::{Dev, MaxRecordSize, Resource},
};

/// Implementation of a record tree.
#[derive(Clone, Debug)]
pub(super) struct Tree<'a, D: Dev, R: Resource> {
	/// Underlying cache.
	cache: &'a Cache<D, R>,
	/// Location of the root of this tree.
	root: RootLocation,
}

#[derive(Clone, Debug)]
enum RootLocation {
	Object { id: u64, root: RootIndex },
	ObjectList,
	ObjectBitmap,
}

impl<'a, D: Dev, R: Resource> Tree<'a, D, R> {
	/// Access one of an object's tree.
	pub fn object(cache: &'a Cache<D, R>, id: u64, root: RootIndex) -> Tree<'a, D, R> {
		Self { cache, root: RootLocation::Object { id, root } }
	}

	/// Access the object list's tree.
	pub fn object_list(cache: &'a Cache<D, R>) -> Tree<'a, D, R> {
		Self { cache, root: RootLocation::ObjectList }
	}

	/// Access the object bitmap's tree.
	pub fn object_bitmap(cache: &'a Cache<D, R>) -> Tree<'a, D, R> {
		Self { cache, root: RootLocation::ObjectBitmap }
	}

	/// Get the maximum record size.
	fn max_rec_size(&self) -> MaxRecordSize {
		self.cache.max_rec_size()
	}

	/// Calculate the upper offset limit for cached entries.
	///
	/// The offset is *exclusive*, i.e. `[0; max_offset)`.
	pub(super) fn max_offset(&self) -> u64 {
		1 << (self.max_rec_size().to_raw() - RECORDREF_SIZE_P2) * (self.depth() as u8)
	}

	/// Get the depth of this tree.
	pub(super) fn depth(&self) -> Depth {
		match &self.root {
			RootLocation::Object { root, .. } => root.depth(),
			RootLocation::ObjectList => self.cache.store.object_list_depth(),
			RootLocation::ObjectBitmap => self.cache.object_bitmap_depth.get(),
		}
	}

	/// Get the root index.
	pub(super) fn root(&self) -> RootIndex {
		match &self.root {
			&RootLocation::Object { root, .. } => root,
			&RootLocation::ObjectList | &RootLocation::ObjectBitmap => RootIndex::I0,
		}
	}

	/// Get the key and index of a record reference in the parent record for the given offset.
	///
	/// # Panics
	///
	/// If the depth is at the maximum (`D3`).
	fn parent_key_index(&self, offset: u64, depth: Depth) -> (Depth, u64, usize) {
		let offt = offset << RECORDREF_SIZE_P2;
		let (offt, index) = util::divmod_p2(offt, self.max_rec_size().to_raw());
		(depth.next(), offt, index)
	}

	/// Get the key and index of the root in the object.
	///
	/// # Panics
	///
	/// If this tree is not referenced by the object list but is a special tree
	/// (e.g. the obvject list itself).
	fn object_key_index(&self) -> (Depth, u64, usize) {
		let &RootLocation::Object { id, .. } = &self.root
			else { panic!("can't get key & offset of object of special tree") };
		let offt = (id << OBJECT_SIZE_P2) + (8 * self.root() as u64);
		let (offt, index) = util::divmod_p2(offt, self.max_rec_size().to_raw());
		(Depth::D0, offt, index)
	}

	pub(super) fn id(&self) -> u64 {
		match &self.root {
			&RootLocation::Object { id, .. } => id,
			&RootLocation::ObjectList => super::OBJECT_LIST_ID,
			&RootLocation::ObjectBitmap => super::OBJECT_BITMAP_ID,
		}
	}

	pub(super) fn id_key(&self, depth: Depth, offset: u64) -> IdKey {
		IdKey { id: self.id(), key: Key::new(self.root(), depth, offset) }
	}
}
