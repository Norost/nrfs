use super::*;

impl Fs {
	pub async fn rename<'a>(&'a self, bg: &Background<'a, FileDev>, job: crate::job::Rename) {
		let (Ok(from_name), Ok(to_name)) = (job.name.as_bytes().try_into(), job.newname.as_bytes().try_into())
				else { return job.reply.error(libc::ENAMETOOLONG) };

		// FIXME for gods sake do it properly.

		// Delete entry at original location first.
		if let Err(e) = self.remove_file(bg, job.newparent, to_name).await {
			if e != libc::ENOENT {
				job.reply.error(e);
				return;
			}
		}

		let self_ino = self.ino.borrow_mut();

		let from_d = self_ino.get_dir(&self.fs, bg, job.parent);
		let to_d = self_ino.get_dir(&self.fs, bg, job.newparent);

		match from_d.transfer(from_name, &to_d, to_name).await.unwrap() {
			Ok(()) => job.reply.ok(),
			Err(TransferError::NotFound) => job.reply.error(libc::ENOENT),
			// On Linux existing entries are overwritten.
			Err(TransferError::Duplicate) => todo!("existing entry should have been removed"),
			Err(TransferError::IsAncestor) => job.reply.error(libc::EINVAL),
			Err(TransferError::Full) => todo!("figure error code for full dir"),
			// This is what Linux returns if you try to create an entry in an unlinked dir.
			Err(TransferError::Dangling) => job.reply.error(libc::ENOENT),
			Err(TransferError::UnknownType) => todo!("figure out error code for unknown type"),
		}
	}
}
