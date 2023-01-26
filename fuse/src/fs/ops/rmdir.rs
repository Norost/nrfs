use super::*;

impl Fs {
	pub async fn rmdir<'a>(&'a self, bg: &Background<'a, FileDev>, job: crate::job::RmDir) {
		let self_ino = self.ino.borrow_mut();

		let d = self_ino.get_dir(&self.fs, bg, job.parent);
		let Ok(name) = job.name.as_bytes().try_into() else { return job.reply.error(libc::ENAMETOOLONG) };

		// Ensure it's a directory because POSIX yadayada
		let Some(e) = d.find(name).await.unwrap() else { return job.reply.error(libc::ENOENT) };
		let r = match &e {
			ItemRef::Dir(_) => Ok(()),
			_ => Err(libc::ENOTDIR),
		};
		e.drop().await.unwrap();
		if let Err(e) = r {
			return job.reply.error(e);
		};

		match d.remove(name).await.unwrap() {
			Ok(()) => job.reply.ok(),
			Err(RemoveError::NotFound) => job.reply.error(libc::ENOENT),
			Err(RemoveError::NotEmpty) => job.reply.error(libc::ENOTEMPTY),
			Err(RemoveError::UnknownType) => job.reply.error(libc::ENOTDIR),
		}
	}
}
