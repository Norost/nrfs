use super::*;

impl Fs {
	pub async fn symlink(&self, job: crate::job::SymLink) {
		let Ok(name) = (&*job.name).try_into() else { return job.reply.error(libc::ENAMETOOLONG) };

		let dir = match self.ino().get(job.parent).unwrap() {
			Get::Key(Key::Dir(d), ..) => self.fs.dir(d),
			Get::Key(..) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let dir = dir.await.unwrap();

		match dir.create_sym(name).await.unwrap() {
			Ok(f) => {
				let ino = self.ino().add(Key::File(f.key()), job.parent, 0);
				let link = job.link;
				f.write_grow(0, &link).await.unwrap().unwrap();
				let attrs = self.init_attrs(&f, job.uid, job.gid, None).await;
				let attr = self.attr(ino, FileType::Symlink, 0, attrs);
				job.reply.entry(&TTL, &attr, 0);
				self.update_gen(ino).await;
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
