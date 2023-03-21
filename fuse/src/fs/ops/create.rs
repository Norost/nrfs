use super::*;

impl Fs {
	pub async fn create(&self, job: crate::job::Create) {
		let Ok(name) = job.name.as_bytes().try_into() else { return job.reply.error(libc::ENAMETOOLONG) };

		let d = self.ino().get_dir(job.parent);
		let d = self.fs.dir(d);

		let ext = mkext(job.mode as _, job.uid, job.gid);
		match d.create_file(name, ext.clone()).await.unwrap() {
			Ok(f) => {
				let ino = self.ino().add_file(f.into_key());
				let attr = self.attr(ino, FileType::RegularFile, 0, ext);
				job.reply.created(&TTL, &attr, 0, 0, 0);
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
