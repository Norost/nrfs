use super::*;

impl Fs {
	pub async fn symlink(&self, job: crate::job::SymLink) {
		let Ok(name) = (&*job.name).try_into() else { return job.reply.error(libc::ENAMETOOLONG) };

		let (dir, lock) = match self.dir_mut(job.parent).await {
			Ok(r) => r,
			Err(e) => return job.reply.error(e),
		};

		match dir.create_sym(name).await.unwrap() {
			Ok(f) => {
				let ino = self.ino().add(Key::File(f.key()), job.parent, self.gen());
				f.set_modified_gen(self.gen()).await.unwrap();
				let link = job.link;
				f.write_grow(0, &link).await.unwrap().unwrap();
				let attrs = self.init_attrs(&f, job.uid, job.gid, None).await;
				let attr = self.attr(
					ino,
					FileType::Symlink,
					link.len().try_into().unwrap(),
					attrs,
				);
				job.reply.entry(&TTL, &attr, 0);
				self.update_gen(job.parent, lock).await;
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
