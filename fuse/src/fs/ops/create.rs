use super::*;

impl Fs {
	pub async fn create<'a>(&'a self, bg: &Background<'a, FileDev>, job: crate::job::Create) {
		let mut self_ino = self.ino.borrow_mut();

		let d = self_ino.get_dir(&self.fs, bg, job.parent);

		let Ok(name) = job.name.as_bytes().try_into() else { return job.reply.error(libc::ENAMETOOLONG) };
		let unix = nrfs::dir::ext::unix::Entry::new(job.mode as _, job.uid, job.gid);
		let mtime = mtime_now();
		let ext =
			nrfs::dir::Extensions { unix: Some(unix), mtime: Some(mtime), ..Default::default() };
		match d.create_file(name, &ext).await.unwrap() {
			Ok(f) => {
				let (ino, f) = self_ino.add_file(f, false);
				if let Some(f) = f {
					f.drop().await.unwrap()
				}
				let data = self_ino.get(&self.fs, bg, ino).data().await.unwrap();
				drop(self_ino);
				job.reply.created(
					&TTL,
					&self.attr(ino, FileType::RegularFile, 0, &data),
					0,
					0,
					0,
				);
			}
			Err(InsertError::Duplicate) => job.reply.error(libc::EEXIST),
			// This is what Linux's tmpfs returns.
			Err(InsertError::Dangling) => job.reply.error(libc::ENOENT),
			Err(InsertError::Full) => todo!("figure out error code"),
		}
	}
}
