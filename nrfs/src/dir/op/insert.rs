use {
	super::{super::InsertError, Dir, Index, Name, Offset},
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Try to insert a new item.
	pub async fn insert(
		&self,
		name: &Name,
		data: &[u8],
	) -> Result<Result<(Offset, Index), InsertError>, Error<D>> {
		trace!("insert {:?} {:x?}", name, data);
		todo!()
	}
}
