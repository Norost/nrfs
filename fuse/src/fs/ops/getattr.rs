use super::*;

impl Fs {
	pub async fn getattr<'a>(&'a self, bg: &Background<'a, FileDev>, job: crate::job::GetAttr) {
		let self_ino = self.ino.borrow_mut();

		let entry = self_ino.get(&self.fs, bg, job.ino);

		// Get type, len
		let (ty, len) = match &*entry {
			ItemRef::Dir(d) => (FileType::Directory, d.len().await.unwrap().into()),
			ItemRef::File(f) => (FileType::RegularFile, f.len().await.unwrap()),
			ItemRef::Sym(f) => (FileType::Symlink, f.len().await.unwrap()),
			ItemRef::Unknown(_) => unreachable!(),
		};

		let data = entry.data().await.unwrap();

		drop(self_ino);
		job.reply.attr(&TTL, &self.attr(job.ino, ty, len, &data));
	}
}
