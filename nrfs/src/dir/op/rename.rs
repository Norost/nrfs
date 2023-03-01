use {
	super::{super::RenameError, Dir, Name},
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Rename an entry
	///
	/// Returns `false` if the entry could not be found or another entry with the same index
	/// exists.
	pub async fn rename(
		&self,
		from: &Name,
		to: &Name,
	) -> Result<Result<(), RenameError>, Error<D>> {
		trace!("rename {:?} -> {:?}", from, to);
		todo!()
	}
}
