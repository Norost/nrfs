use crate::{resource::Buf, Dev, Error, Resource};

impl<'a, D: Dev, R: Resource> super::Object<'a, D, R> {
	/// Set a leaf record's data directly.
	///
	/// This avoids a fetch if the entry isn't already present.
	///
	/// `offset` is expressed in record units, not bytes!
	pub(super) async fn set(&self, offset: u64, data: R::Buf) -> Result<(), Error<D>> {
		trace!("set {:#x} {} data.len {}", self.id, offset, data.len(),);

		let (root, offt) = self
			.offset_to_tree(offset)
			.expect("offset is not addressable");

		super::super::Tree::object(self.cache, self.id, root)
			.set(offt, data)
			.await
	}
}
