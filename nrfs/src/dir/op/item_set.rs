use {
	super::{
		super::{item::OFFT_DATA, Type},
		Dir, Index, Name, Offset,
	},
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Set arbitrary item data.
	pub async fn item_set(&self, index: Index, offset: u16, data: &[u8]) -> Result<(), Error<D>> {
		trace!("set {:?}+{} {:x?}", item_index, offset, data);
		let offt = u64::from(index) + u64::from(offset);
		self.fs.get(self.id).await?.write(offt, data).await?;
		Ok(())
	}

	/// Set the type of an item.
	pub async fn item_set_ty(&self, index: Index, ty: Type) -> Result<(), Error<D>> {
		trace!("set_ty {:?} {:?}", offset, ty);
		self.item_set(index, OFFT_DATA, &ty.to_raw()).await
	}
}
