use super::*;

impl Fs {
	pub async fn lookup(&self, job: crate::job::Lookup) {
		let Ok(name) = job.name.as_bytes().try_into() else { return job.reply.error(libc::ENAMETOOLONG) };

		let d = self.ino().get_dir(job.parent);
		let d = self.fs.dir(d);

		let Some(item) = d.search(name).await.unwrap()
			else { return job.reply.error(libc::ENOENT) };

		let (ty, len, ino) = match item.key() {
			ItemKey::Dir(d) => {
				let ino = self.ino().add_dir(d);
				(FileType::Directory, 0, ino)
			}
			ItemKey::File(f) => {
				let len = self.fs.file(f).len().await.unwrap();
				let ino = self.ino().add_file(f);
				(FileType::RegularFile, len, ino)
			}
			ItemKey::Sym(f) => {
				let len = self.fs.file(f).len().await.unwrap();
				let ino = self.ino().add_sym(f);
				(FileType::Symlink, len, ino)
			}
		};

		job.reply.entry(&TTL, &self.attr(ino, ty, len, item.ext), 0)
	}
}
