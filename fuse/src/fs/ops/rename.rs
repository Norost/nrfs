use super::*;

impl Fs {
	pub async fn rename(&self, job: crate::job::Rename) {
		let (Ok(from_name), Ok(to_name)) = (job.name.as_bytes().try_into(), job.newname.as_bytes().try_into())
				else { return job.reply.error(libc::ENAMETOOLONG) };

		let from_d = self.ino().get_dir(job.parent);
		let from_d = self.fs.dir(from_d);
		let to_d = self.ino().get_dir(job.newparent);
		let to_d = self.fs.dir(to_d);

		let Some(from_item) = from_d.search(from_name).await.unwrap()
			else { return job.reply.error(libc::ENOENT) };

		if let Some(to_item) = to_d.search(to_name).await.unwrap() {
			let ino = self.ino().get_ino(to_item.key());
			if let Some(ino) = ino {
				self.fs.item(to_item.key()).erase_name().await.unwrap();
				self.ino().mark_unlinked(ino);
			} else {
				match to_item.key() {
					ItemKey::Dir(d) => match self.fs.dir(d).destroy().await {
						_ => todo!(),
					},
					ItemKey::File(f) | ItemKey::Sym(f) => self.fs.file(f).destroy().await.unwrap(),
				}
			}
		}

		match self
			.fs
			.item(from_item.key())
			.transfer(&to_d, to_name)
			.await
			.unwrap()
		{
			Ok(key) => {
				let mut inoref = self.ino();
				if let Some(ino) = inoref.get_ino(from_item.key()) {
					inoref.set(ino, key);
				}
				job.reply.ok();
			}
			Err(TransferError::Full) => todo!(),
			Err(TransferError::IsRoot) => unreachable!(),
			Err(TransferError::Duplicate) => unreachable!(),
		}
	}
}
