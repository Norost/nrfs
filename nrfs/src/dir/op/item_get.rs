use {
	super::{Dir, Offset},
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Get raw item data.
	pub(crate) async fn item_get(
		&self,
		item_index: Index,
		offset: u16,
		buf: &mut [u8],
	) -> Result<(), Error<D>> {
		trace!("get {:?}+{} {:?}", item_index, offset, buf.len());
		let offt = u64::from(item_index) + u64::from(offset);
		self.fs.get(self.id).await?.read(offt, buf).await?;
		Ok(())
	}
}
