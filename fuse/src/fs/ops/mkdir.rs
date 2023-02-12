use super::*;

impl Fs {
	pub async fn mkdir(&self, job: crate::job::MkDir) {
		let mut self_ino = self.ino.borrow_mut();

		let d = self_ino.get_dir(&self.fs, job.parent);

		let Ok(name) = job.name.as_bytes().try_into() else { return job.reply.error(libc::ENAMETOOLONG) };

		let unix = nrfs::dir::ext::unix::Entry::new(job.mode as _, job.uid, job.gid);
		let mtime = mtime_now();
		let ext =
			nrfs::dir::Extensions { unix: Some(unix), mtime: Some(mtime), ..Default::default() };
		let opt = nrfs::DirOptions {
			extensions: *nrfs::dir::EnableExtensions::default()
				.add_unix()
				.add_mtime(),
			..nrfs::dir::DirOptions::new(&[0; 16]) // FIXME randomize
		};
		match d.create_dir(name, &opt, &ext).await.unwrap() {
			Ok(dd) => {
				let data = dd.data().await.unwrap();
				let (ino, e) = self_ino.add_dir(dd);
				if let Some(e) = e {
					e.drop().await.unwrap();
				}
				drop(self_ino);
				let attr = self.attr(ino, FileType::Directory, 0, &data);
				job.reply.entry(&TTL, &attr, 0);
			}
			Err(InsertError::Duplicate) => job.reply.error(libc::EEXIST),
			// This is what Linux's tmpfs returns.
			Err(InsertError::Dangling) => job.reply.error(libc::ENOENT),
			Err(InsertError::Full) => todo!("figure out error code"),
		}
	}
}
