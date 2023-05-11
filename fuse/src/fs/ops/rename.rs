use super::*;

impl Fs {
	pub async fn rename(&self, job: crate::job::Rename) {
		let (Ok(from_name), Ok(to_name)) = ((&*job.name).try_into(), (&*job.newname).try_into())
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let from_d = match self.ino().get(job.parent).unwrap() {
			Get::Key(Key::Dir(d), ..) => self.fs.dir(d).await.unwrap(),
			Get::Key(..) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let to_d = match self.ino().get(job.newparent).unwrap() {
			Get::Key(Key::Dir(d), ..) => self.fs.dir(d).await.unwrap(),
			Get::Key(..) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};

		let Some(from_item) = from_d.search(from_name).await.unwrap()
			else { return job.reply.error(libc::ENOENT) };

		if let Some(to_item) = to_d.search(to_name).await.unwrap() {
			match to_d.remove(to_item.key).await.unwrap() {
				Ok(()) => {}
				Err(nrfs::RemoveError::NotEmpty) => return job.reply.error(libc::ENOTEMPTY),
			}
			let ino = self.ino().get_ino(to_item.key);
			if let Some(ino) = ino {
				self.ino().mark_stale(ino);
			}
		}

		match from_d
			.transfer(from_item.key, &to_d, to_name)
			.await
			.unwrap()
		{
			Ok(key) => {
				let ino = self.ino().get_ino(from_item.key);
				ino.map(|ino| self.ino().set(ino, key, job.newparent));
				job.reply.ok();
				self.update_gen(ino.unwrap()).await; //FIXME unwrap
				self.update_gen(job.parent).await;
			}
			Err(nrfs::TransferError::Duplicate) => unreachable!(),
			Err(nrfs::TransferError::Full) => todo!(),
		}
	}
}
