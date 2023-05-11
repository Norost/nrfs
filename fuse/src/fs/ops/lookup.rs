use super::*;

impl Fs {
	pub async fn lookup(&self, job: crate::job::Lookup) {
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let dir = match self.ino().get(job.parent).unwrap() {
			Get::Key(Key::Dir(d), ..) => self.fs.dir(d).await.unwrap(),
			Get::Key(..) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};

		let Some(item) = dir.search(name).await.unwrap()
			else { return job.reply.error(libc::ENOENT) };

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
