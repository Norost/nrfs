use super::*;

impl Fs {
	pub async fn getattr(&self, job: crate::job::GetAttr) {
		let _lock = self.lock(job.ino).await;
		let key = match self.ino().get(job.ino).unwrap() {
			Get::Key(k, ..) => k,
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let item = self.fs.item(*key.key());

		let len = item.len().await.unwrap();
		let ty = match key {
			Key::Dir(_) => FileType::Directory,
			Key::File(_) => FileType::RegularFile,
			Key::Sym(_) => FileType::Symlink,
		};

		let attrs = get_attrs(&item).await;
		job.reply.attr(&TTL, &self.attr(job.ino, ty, len, attrs));
	}
}
