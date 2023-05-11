use super::*;

impl Fs {
	pub async fn create(&self, job: crate::job::Create) {
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let dir = match self.ino().get(job.parent).unwrap() {
			Get::Key(Key::Dir(d)) => self.fs.dir(d).await.unwrap(),
			Get::Key(_) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};

		match dir.create_file(name).await.unwrap() {
			Ok(f) => {
				let attrs = init_attrs(&f, job.uid, job.gid, Some(job.mode as u16 & 0o777)).await;
				let ino = self.ino().add(Key::File(f.key()));
				let attr = self.attr(ino, FileType::RegularFile, 0, attrs);
				job.reply.created(&TTL, &attr, 0, 0, 0);
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
