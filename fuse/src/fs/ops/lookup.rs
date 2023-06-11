use super::*;

impl Fs {
	pub async fn lookup(&self, job: crate::job::Lookup) {
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };

		// FIXME we do need to acquire a lock here
		// However, we risk a deadlock, so don't for now.
		let (dir, _) = match self.dir(job.parent).await {
			Ok(r) => r,
			Err(e) => return job.reply.error(e),
		};

		let Some(item) = dir.search(name).await.unwrap()
			else { return job.reply.error(libc::ENOENT) };
		let ino = self.ino().get_ino(item.key);
		let _lock = if let Some(ino) = ino {
			Some(self.lock(ino).await)
		} else {
			None
		};

		let len = self.fs.item(item.key).len().await.unwrap();
		let (ty, key) = match item.ty {
			ItemTy::Dir => (FileType::Directory, Key::Dir(item.key)),
			ItemTy::File | ItemTy::EmbedFile => (FileType::RegularFile, Key::File(item.key)),
			ItemTy::Sym | ItemTy::EmbedSym => (FileType::Symlink, Key::Sym(item.key)),
		};
		let m = self.fs.item(item.key).modified().await.unwrap();
		let ino = self.ino().add(key, job.parent, m.gen);

		let attrs = get_attrs(&self.fs.item(item.key)).await;
		job.reply.entry(&TTL, &self.attr(ino, ty, len, attrs), 0)
	}
}
