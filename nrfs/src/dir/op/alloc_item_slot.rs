use {
	super::{Dir, Index, Name, Offset},
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Allocate an item slot.
	pub async fn alloc_item_slot(&self) -> Result<Option<(Offset, Index)>, Error<D>> {
		trace!("alloc_item_slot");
		todo!()
	}
}
