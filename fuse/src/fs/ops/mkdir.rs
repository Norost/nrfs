use super::*;

impl Fs {
	pub async fn mkdir(&self, job: crate::job::MkDir) {
		let Ok(name) = (&*job.name).try_into() else { return job.reply.error(libc::ENAMETOOLONG) };

		let dir = match self.ino().get(job.parent).unwrap() {
			Get::Key(Key::Dir(d)) => self.fs.dir(d).await.unwrap(),
			Get::Key(_) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};

		match dir.create_dir(name).await.unwrap() {
			Ok(d) => {
				let attrs = init_attrs(&d, job.uid, job.gid, Some(job.mode as u16 & 0o777)).await;
				let ino = self.ino().add(Key::Dir(d.key()));
				let attr = self.attr(ino, FileType::Directory, 0, attrs);
				job.reply.entry(&TTL, &attr, 0);
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
