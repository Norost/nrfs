use {
	super::{Dir, Index, Name},
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Find an entry by name.
	///
	/// Returns index & raw data associated with the item.
	pub async fn find(&self, name: &Name) -> Result<Option<(Index, Vec<u8>)>, Error<D>> {
		let d = self.fs.dir_data(self.id);
		let buf = &mut [0; 16];
		loop {
			todo!()
		}
	}
}
