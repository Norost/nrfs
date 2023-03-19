use super::*;

impl Fs {
	pub async fn symlink(&self, job: crate::job::SymLink) {
		let Ok(name) = job.name.as_bytes().try_into() else { return job.reply.error(libc::ENAMETOOLONG) };

		if self.ino().is_unlinked(job.parent) {
			return job.reply.error(libc::ENOENT);
		}

		let d = self.ino().get_dir(job.parent);
		let d = self.fs.dir(d);

		let ext = mkext(0o777, job.uid, job.gid);
		match d.create_sym(name, ext.clone()).await.unwrap() {
			Ok(f) => {
				let link = job.link.as_os_str().as_bytes();
				f.write_grow(0, link).await.unwrap().unwrap();
				let ino = self.ino().add_sym(f.into_key());
				let attr = self.attr(ino, FileType::Symlink, link.len() as _, ext);
				job.reply.entry(&TTL, &attr, 0);
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
