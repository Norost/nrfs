use {super::*, nrfs::ItemTy};

impl Fs {
	pub async fn unlink(&self, job: crate::job::Unlink) {
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let (dir, lock) = match self.dir_mut(job.parent).await {
			Ok(r) => r,
			Err(e) => return job.reply.error(e),
		};

		let Some(item) = dir.search(name).await.unwrap()
			else { return job.reply.error(libc::ENOENT) };
		if item.ty == ItemTy::Dir {
			return job.reply.error(libc::EISDIR);
		}

		// This is safe with rename as we hold the directory lock
		let ino = self.ino().get_ino(item.key);
		let lock_item = if let Some(ino) = ino {
			Some(self.lock_mut(ino).await)
		} else {
			None
		};

		dir.remove(item.key).await.unwrap().unwrap();

		if let Some(ino) = ino {
			self.ino().mark_stale(ino);
		}
		drop(lock_item);

		job.reply.ok();
		self.update_gen(job.parent, lock).await;
	}
}
