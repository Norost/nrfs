use super::*;

impl Fs {
	pub async fn getattr(&self, job: crate::job::GetAttr) {
		let item = self.ino().get(job.ino);
		let item = self.fs.item(item);

		// Get type, len
		let (ty, len) = match item.key() {
			ItemKey::Dir(_) => (FileType::Directory, 0),
			ItemKey::File(f) => (FileType::RegularFile, self.fs.file(f).len().await.unwrap()),
			ItemKey::Sym(f) => (FileType::Symlink, self.fs.file(f).len().await.unwrap()),
		};
		let ext = item.ext().await.unwrap();

		job.reply.attr(&TTL, &self.attr(job.ino, ty, len, ext));
	}
}
