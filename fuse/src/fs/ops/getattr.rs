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
		let attrs = get_attrs(&item).await;
		let ty = match key {
			Key::Dir(_) => FileType::Directory,
			Key::Sym(_) => FileType::Symlink,
			Key::File(_) => getty(attrs.mode.unwrap_or(0)).unwrap_or(FileType::RegularFile),
		};

		job.reply.attr(&TTL, &self.attr(job.ino, ty, len, attrs));
	}
}
