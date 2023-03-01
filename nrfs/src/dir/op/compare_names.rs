use {
	super::{Dir, Name, Offset},
	crate::{Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Compare an item's name with the given name.
	pub async fn compare_names(&self, item_name: &[u8; 16], name: &Name) -> Result<bool, Error<D>> {
		trace!("compare_names {:x?} {:?}", item_name, name);
		if item_name[0] != name.len_u8() {
			return Ok(false);
		}
		if name.len_u8() <= 15 {
			Ok(&item_name[1..1 + name.len()] == &**name)
		} else if item_name[1..10] != name[..9] {
			Ok(false)
		} else {
			let offset = Offset::from_raw(item_name[10..].try_into().unwrap());
			let buf = &mut *vec![0; name.len() - 9];
			self.read_heap(offset, buf).await?;
			Ok(buf == &name[9..])
		}
	}
}
