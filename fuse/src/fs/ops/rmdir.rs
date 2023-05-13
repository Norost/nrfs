use super::*;

impl Fs {
	pub async fn rmdir(&self, job: crate::job::RmDir) {
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let (dir, lock) = match self.dir_mut(job.parent).await {
			Ok(r) => r,
			Err(e) => return job.reply.error(e),
		};

		let Some(item) = dir.search(name).await.unwrap()
			else { return job.reply.error(libc::ENOENT) };
		if item.ty != nrfs::ItemTy::Dir {
			return job.reply.error(libc::ENOTDIR);
		}

		let ino = self.ino().get_ino(item.key);
		// FIXME potential deadlock with rename
		let lock_item = if let Some(ino) = ino {
			Some(self.lock_mut(ino).await)
		} else {
			None
		};

		match dir.remove(item.key).await.unwrap() {
			Ok(()) => {}
			Err(nrfs::RemoveError::NotEmpty) => return job.reply.error(libc::ENOTEMPTY),
		}
		drop(lock_item);

		if let Some(ino) = ino {
			self.ino().mark_stale(ino);
		}
		job.reply.ok();
		self.update_gen(job.parent, lock).await;
	}
}
