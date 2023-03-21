use {
	super::{
		super::{DirNewItem, TransferError},
		Dir, FindResult, Name,
	},
	crate::{item::ItemData, Dev, Error},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Move an entry to another directory.
	///
	/// The returned value can be used to update cached data.
	pub(crate) async fn item_transfer<'n>(
		&self,
		from_index: u32,
		to_dir: &Dir<'a, D>,
		to_name: &'n Name,
	) -> Result<Result<u32, TransferError>, Error<D>> {
		trace!(
			"transfer {:#x} {} -> {:#x} {:?}",
			self.key.id,
			from_index,
			to_dir.key.id,
			to_name
		);

		let mut from_hdr = self.header().await?;

		let mut to_hdr = None;
		let to_hdr_ref = if self.key.id == to_dir.key.id {
			&from_hdr
		} else {
			to_hdr.insert(to_dir.header().await?)
		};
		let to_index = match to_dir.find(to_hdr_ref, to_name).await? {
			FindResult::Found { .. } => return Ok(Err(TransferError::Duplicate)),
			FindResult::NotFound { data_index } => data_index,
		};

		let (mut data, ext) = self.item_get_data_ext(&from_hdr, from_index).await?;
		self.item_erase(&mut from_hdr, from_index).await?;

		let embed = match data.ty {
			ItemData::TY_DIR | ItemData::TY_FILE | ItemData::TY_SYM => None,
			ItemData::TY_EMBED_FILE | ItemData::TY_EMBED_SYM => {
				let mut buf = vec![0; data.len.try_into().unwrap()];
				let heap = self.heap(&from_hdr);
				heap.read(data.id_or_offset, &mut buf).await?;
				heap.dealloc(&mut from_hdr, data.id_or_offset, data.len)
					.await?;
				Some(buf)
			}
			_ => todo!(),
		};

		let mut to_hdr = if self.key.id == to_dir.key.id {
			from_hdr
		} else {
			self.set_header(from_hdr).await?;
			to_hdr.unwrap()
		};

		if let Some(embed) = embed {
			let heap = to_dir.heap(&to_hdr);
			data.id_or_offset = heap.alloc(&mut to_hdr, data.len).await?;
			heap.write(data.id_or_offset, &embed).await?;
		}

		let item = DirNewItem { name: to_name, data, ext };
		let to_index = to_dir.item_insert(&mut to_hdr, to_index, item).await?;

		to_dir.set_header(to_hdr).await?;

		Ok(Ok(to_index))
	}
}
