use {super::*, nrfs::EnableExt};

impl Fs {
	pub async fn mkdir(&self, job: crate::job::MkDir) {
		let Ok(name) = job.name.as_bytes().try_into() else { return job.reply.error(libc::ENAMETOOLONG) };

		if self.ino().is_unlinked(job.parent) {
			return job.reply.error(libc::ENOENT);
		}

		let d = self.ino().get_dir(job.parent);
		let d = self.fs.dir(d);

		let ext = mkext(job.mode as _, job.uid, job.gid);
		let enable_ext = *EnableExt::default().add_unix().add_mtime();

		match d.create_dir(name, enable_ext, ext.clone()).await.unwrap() {
			Ok(dd) => {
				let ino = self.ino().add_dir(dd.into_key());
				let attr = self.attr(ino, FileType::Directory, 0, ext);
				job.reply.entry(&TTL, &attr, 0);
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
