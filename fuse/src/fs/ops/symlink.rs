use super::*;

impl Fs {
	pub async fn symlink(&self, job: crate::job::SymLink) {
		let Ok(name) = job.name.as_bytes().try_into() else { return job.reply.error(libc::ENAMETOOLONG) };

		let dir = match self.ino().get(job.parent).unwrap() {
			Get::Key(Key::Dir(d)) => self.fs.dir(d).await.unwrap(),
			Get::Key(_) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};

		match dir.create_sym(name).await.unwrap() {
			Ok(f) => {
				let ino = self.ino().add(Key::File(f.key()));
				let link = job.link.as_os_str().as_bytes();
				f.write_grow(0, link).await.unwrap().unwrap();
				let attrs = init_attrs(&f, job.uid, job.gid, None).await;
				let attr = self.attr(ino, FileType::Symlink, 0, attrs);
				job.reply.entry(&TTL, &attr, 0);
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
