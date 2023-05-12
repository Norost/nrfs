use super::*;

impl Fs {
	pub async fn rmdir(&self, job: crate::job::RmDir) {
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let dir = match self.ino().get(job.parent).unwrap() {
			Get::Key(Key::Dir(d), ..) => self.fs.dir(d),
			Get::Key(..) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let dir = dir.await.unwrap();

		let Some(item) = dir.search(name).await.unwrap()
			else { return job.reply.error(libc::ENOENT) };
		if item.ty != nrfs::ItemTy::Dir {
			return job.reply.error(libc::ENOTDIR);
		}
		match dir.remove(item.key).await.unwrap() {
			Ok(()) => {}
			Err(nrfs::RemoveError::NotEmpty) => return job.reply.error(libc::ENOTEMPTY),
		}

		let ino = self.ino().get_ino(item.key);
		if let Some(ino) = ino {
			self.ino().mark_stale(ino);
		}
		job.reply.ok();
		self.update_gen(job.parent).await;
	}
}
