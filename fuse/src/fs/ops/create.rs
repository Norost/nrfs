use super::*;

impl Fs {
	pub async fn create(&self, job: crate::job::Create) {
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let (dir, lock) = match self.dir_mut(job.parent).await {
			Ok(r) => r,
			Err(e) => return job.reply.error(e),
		};

		match dir.create_file(name).await.unwrap() {
			Ok(f) => {
				let attrs = self
					.init_attrs(&f, job.uid, job.gid, Some(job.mode as u16 & 0o777))
					.await;
				let ino = self.ino().add(Key::File(f.key()), job.parent, self.gen());
				f.set_modified_gen(self.gen()).await.unwrap();
				let attr = self.attr(ino, FileType::RegularFile, 0, attrs);
				job.reply.created(&TTL, &attr, 0, 0, 0);
				self.update_gen(job.parent, lock).await;
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
