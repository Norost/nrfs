use {
	super::{Dir, Index},
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Set any item data at an arbitrary offset.
	pub async fn write_item(&self, index: Index, offset: u16, data: &[u8]) -> Result<(), Error<D>> {
		trace!("set {:?} {:?}:{:?}", index, offset, data.len());
		todo!()
	}
}
