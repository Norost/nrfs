use {
	super::{
		super::{EntryRef, Tree},
		Object,
	},
	crate::{data::record::Depth, Dev, Error, Resource},
};

impl<'a, D: Dev, R: Resource> Object<'a, D, R> {
	/// Fetch an entry and return a reference to it.
	///
	/// `offset` is calculated in record units.
	///
	/// # Notes
	///
	/// [`EntryRef`] must be dropped before an await point is reached!
	///
	/// `offset` is *not* bounds-checked. It is up to the caller to ensure it does not exceed
	/// the total length of the object.
	///
	/// # Panics
	///
	/// If no root can address the given offset.
	///
	/// If `offset >= 2**55`
	pub async fn get(&self, offset: u64) -> Result<EntryRef<'a, D, R>, Error<D>> {
		trace!("get {:#x} {:?}", self.id, offset);

		let (root, offt) = self
			.offset_to_tree(offset)
			.expect("offset is not addressable");

		Tree::object(self.cache, self.id, root)
			.get(Depth::D0, offt)
			.await
	}
}
