use super::*;

impl Fs {
	pub async fn mkdir(&self, job: crate::job::MkDir) {
		let Ok(name) = (&*job.name).try_into() else { return job.reply.error(libc::ENAMETOOLONG) };

		let (dir, lock) = match self.dir_mut(job.parent).await {
			Ok(r) => r,
			Err(e) => return job.reply.error(e),
		};

		match dir.create_dir(name).await.unwrap() {
			Ok(d) => {
				let attrs = self
					.init_attrs(&d, job.uid, job.gid, Some(job.mode as u16 & 0o777))
					.await;
				let ino = self.ino().add(Key::Dir(d.key()), job.parent, self.gen());
				d.set_modified_gen(self.gen()).await.unwrap();
				let attr = self.attr(ino, FileType::Directory, 0, attrs);
				job.reply.entry(&TTL, &attr, 0);
				self.update_gen(job.parent, lock).await;
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
