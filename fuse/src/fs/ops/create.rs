use super::*;

impl Fs {
	pub async fn create(&self, job: crate::job::Create) {
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let dir = match self.ino().get(job.parent).unwrap() {
			Get::Key(Key::Dir(d), ..) => self.fs.dir(d),
			Get::Key(..) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let dir = dir.await.unwrap();

		match dir.create_file(name).await.unwrap() {
			Ok(f) => {
				let attrs = self
					.init_attrs(&f, job.uid, job.gid, Some(job.mode as u16 & 0o777))
					.await;
				let ino = self.ino().add(Key::File(f.key()), job.parent, 0);
				let attr = self.attr(ino, FileType::RegularFile, 0, attrs);
				job.reply.created(&TTL, &attr, 0, 0, 0);
				self.update_gen(ino).await;
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
