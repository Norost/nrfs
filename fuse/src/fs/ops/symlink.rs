use super::*;

impl Fs {
	pub async fn symlink<'a>(&'a self, bg: &Background<'a, FileDev>, job: crate::job::SymLink) {
		let mut self_ino = self.ino.borrow_mut();

		let d = self_ino.get_dir(&self.fs, bg, job.parent);
		let Ok(name) = job.name.as_bytes().try_into() else { return job.reply.error(libc::ENAMETOOLONG) };
		let unix = nrfs::dir::ext::unix::Entry::new(0o777, job.uid, job.gid);
		let mtime = mtime_now();
		let ext =
			nrfs::dir::Extensions { unix: Some(unix), mtime: Some(mtime), ..Default::default() };
		match d.create_sym(name, &ext).await.unwrap() {
			Ok(f) => {
				let link = job.link.as_os_str().as_bytes();
				f.write_grow(0, link).await.unwrap();
				let (ino, f) = self_ino.add_sym(f, false);
				if let Some(f) = f {
					f.drop().await.unwrap()
				}
				let data = self_ino.get(&self.fs, bg, ino).data().await.unwrap();
				drop(self_ino);
				let attr = self.attr(ino, FileType::Symlink, link.len() as _, &data);
				job.reply.entry(&TTL, &attr, 0);
			}
			Err(InsertError::Duplicate) => job.reply.error(libc::EEXIST),
			// This is what Linux's tmpfs returns.
			Err(InsertError::Dangling) => job.reply.error(libc::ENOENT),
			Err(InsertError::Full) => todo!("figure out error code"),
		}
	}
}
