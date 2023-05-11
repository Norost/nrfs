use {super::*, nrfs::ItemTy};

impl Fs {
	pub async fn unlink(&self, job: crate::job::Unlink) {
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let dir = match self.ino().get(job.parent).unwrap() {
			Get::Key(Key::Dir(d)) => self.fs.dir(d).await.unwrap(),
			Get::Key(_) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};

		let Some(item) = dir.search(name).await.unwrap()
			else { return job.reply.error(libc::ENOENT) };
		if item.ty == ItemTy::Dir {
			return job.reply.error(libc::EISDIR);
		}
		dir.remove(item.key).await.unwrap().unwrap();

		let ino = self.ino().get_ino(item.key);
		if let Some(ino) = ino {
			self.ino().mark_stale(ino);
		}
		job.reply.ok();
	}
}
