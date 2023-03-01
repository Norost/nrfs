use {
	super::{Dir, Index, Name, Offset},
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Deallocate an item slot.
	pub async fn dealloc_item_slot(&self, index: Index) -> Result<(), Error<D>> {
		trace!("dealloc_item_slot {:?}", index);
		todo!()
	}
}
