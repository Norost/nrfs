use {
	super::{EntryRef, Key, Tree},
	crate::{Dev, Error, Resource},
};

impl<'a, 'b, D: Dev, R: Resource> Tree<'a, 'b, D, R> {
	/// fetch an entry and return a reference to it.
	///
	/// `offset` is calculated in record units.
	///
	/// If `offset` is out of range, `None` is returned.
	///
	/// # Notes
	///
	/// [`EntryRef`] must be dropped before an await point is reached!
	///
	/// While `offset` is bounds-checked, the length of the record itself is not checked.
	pub async fn view(&self, offset: u64) -> Result<Option<EntryRef<'a, D, R>>, Error<D>> {
		let key = Key::new(0, self.id, 0, offset);
		trace!("view {:?}", key);

		// Check limits
		let len = self.len().await?;
		let Some(end) = len.checked_sub(1) else { return Ok(None) };
		let max_offset = end >> self.max_record_size().to_raw();
		if offset > max_offset {
			return Ok(None);
		}

		Ok(Some(self.get(0, offset).await?))
	}
}
