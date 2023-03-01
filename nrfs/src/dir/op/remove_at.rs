use {
	super::{super::RemoveError, Dir, Index},
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Remove a specific entry.
	pub async fn remove_at(&self, index: Index) -> Result<Result<(), RemoveError>, Error<D>> {
		trace!("remove_at {:?}", index);
		todo!()
	}
}
