use {
	super::{super::TransferError, Dir, Index, Name, Offset},
	crate::{Dev, Error, ObjectId},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Move an entry to another directory.
	pub async fn transfer(
		&self,
		name: &Name,
		to_dir: ObjectId,
		to_name: &Name,
	) -> Result<Result<(), TransferError>, Error<D>> {
		trace!("transfer {:?} {:?} {:?}", name, to_dir, to_name);

		todo!();
		/*
		// 1. Find the entry + item to transfer.
		// 2. (if embedded) allocate on heap in other dir & update item.
		// 3. Try to insert entry + item in other dir.
		// 4. (if embedded) copy to other dir & deallocate in current.
		// 5. Remove entry + item in this dir.
		// 6. Transfer child, if present.

		if self.id == to_dir {
			// Don't transfer, rename instead.
			return Ok(match self.rename(name, to_name).await? {
				Ok(()) => Ok(()),
				Err(RenameError::NotFound) => Err(TransferError::NotFound),
				Err(RenameError::Duplicate) => Err(TransferError::Duplicate),
			});
		}

		let to_dir = Dir::new(self.fs, to_dir);

		let from_map = self.hashmap().await?;

		// 1. Find the entry + item to transfer.
		let Some(entry) = from_map.find_index(name).await?
			else { return Ok(Err(TransferError::NotFound)) };
		let mut item = self.get(entry.item_index).await?;
		debug_assert!(item.data.key.is_some(), "item to transfer is not in use");

		// If we don't know the type, don't transfer to avoid bringing the filesystem in an
		// inconsistent state.
		match item.ty {
			Type::None => todo!("none type (corrupt fs?)"),
			Type::Unknown(_) => return Ok(Err(TransferError::UnknownType)),
			_ => {}
		}

		// If the entry is a directory, ensure it is not a ancestor of to_dir
		if let Type::Dir { id } = item.ty {
			// Start from to_dir and work downwards to the root.
			// The root is guaranteed to be the ancestor of all other objects.
			let mut cur_id = to_dir.id;
			while cur_id != 0 {
				if cur_id == id {
					// to_dir is a descendant of the entry to be moved, so cancel operation.
					return Ok(Err(TransferError::IsAncestor));
				}
				cur_id = self.fs.dir_data(cur_id).header.parent_id;
			}
		}

		// 2. (if embedded) allocate on heap in other dir & update item.
		let from_embed_data = match &mut item.ty {
			Type::EmbedFile { offset, length } | Type::EmbedSym { offset, length } => {
				let from_offset = *offset;
				let to_offset = to_dir.alloc_heap((*length).into()).await?;
				*offset = to_offset;
				Some((from_offset, to_offset, *length))
			}
			Type::Dir { .. } | Type::File { .. } | Type::Sym { .. } => None,
			Type::Unknown(_) | Type::None => unreachable!(),
		};

		// 3. Try to insert entry + item in other dir.
		let ext = Extensions { unix: item.data.ext_unix, mtime: item.data.ext_mtime };
		let to_index = match to_dir.insert(to_name, item.ty, &ext).await? {
			Ok(i) => i,
			Err(InsertError::Full) => return Ok(Err(TransferError::Full)),
			Err(InsertError::Duplicate) => return Ok(Err(TransferError::Duplicate)),
			Err(InsertError::Dangling) => return Ok(Err(TransferError::Dangling)),
		};

		// 4. (if embedded) copy to other dir & deallocate in current.
		if let Some((from_offset, to_offset, length)) = from_embed_data {
			let buf = &mut vec![0; length.into()];
			self.read_heap(from_offset, buf).await?;
			self.dealloc_heap(from_offset, length.into()).await?;
			to_dir.write_heap(to_offset, buf).await?;
		}

		// 5. Remove entry + item in this dir.
		from_map.remove_at(entry.index).await?;
		self.dealloc_item_slot(item.index).await?;
		let item_len = self.fs.dir_data(self.id).item_size();
		self.set(item.index, 0, &vec![0; item_len.into()]).await?;
		// Deallocate key if stored on heap
		match entry.key {
			None | Some(Key::Embed { .. }) => {}
			Some(Key::Heap { offset, len, .. }) => {
				self.dealloc_heap(offset, len.get().into()).await?
			}
		}
		self.update_item_count(|x| x - 1).await?;

		// 6. Transfer child, if present.
		let mut data = self.fs.dir_data(self.id);
		if let Some(child) = data.children.remove(&item.index) {
			// Dereference current dir
			data.header.reference_count -= 1;
			drop(data);

			// Move to other dir and increase refcount
			let mut data = self.fs.dir_data(to_dir.id);
			data.children.insert(to_index, child);
			data.header.reference_count += 1;
			drop(data);

			// Fixup child
			let mut header = match child {
				Child::File(idx) => {
					let mut data = self.fs.file_data(idx);
					if let Some((_, offset, length)) = from_embed_data {
						// Fixup pointer to embedded data.
						debug_assert!(matches!(&data.inner, file::Inner::Embed { .. }));
						data.inner = file::Inner::Embed { offset, length };
					} else {
						debug_assert!(matches!(&data.inner, file::Inner::Object { .. }));
					}
					RefMut::map(data, |d| &mut d.header)
				}
				Child::Dir(id) => {
					debug_assert!(from_embed_data.is_none(), "dir is never embedded");
					RefMut::map(self.fs.dir_data(id), |d| &mut d.header)
				}
			};
			header.parent_id = to_dir.id;
			header.parent_index = to_index;
		}

		Ok(Ok(()))
			*/
	}
}
