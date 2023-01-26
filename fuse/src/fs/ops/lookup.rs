use super::*;

impl Fs {
	pub async fn lookup<'a>(&'a self, bg: &Background<'a, FileDev>, job: crate::job::Lookup) {
		let mut self_ino = self.ino.borrow_mut();

		let d = self_ino.get_dir(&self.fs, bg, job.parent);

		let Ok(name) = job.name.as_bytes().try_into() else { return job.reply.error(libc::ENAMETOOLONG) };
		let Some(entry) = d.find(name).await.unwrap() else { return job.reply.error(libc::ENOENT) };

		let data = entry.data().await.unwrap();

		// Get type, len, add to inode store
		let (ty, len, ino) = match entry {
			ItemRef::Dir(d) => {
				let len = d.len().await.unwrap().into();
				let (ino, e) = self_ino.add_dir(d, true);
				if let Some(e) = e {
					e.drop().await.unwrap()
				}
				(FileType::Directory, len, ino)
			}
			ItemRef::File(f) => {
				let len = f.len().await.unwrap();
				let (ino, e) = self_ino.add_file(f, true);
				if let Some(e) = e {
					e.drop().await.unwrap()
				}
				(FileType::RegularFile, len, ino)
			}
			ItemRef::Sym(f) => {
				let len = f.len().await.unwrap();
				let (ino, e) = self_ino.add_sym(f, true);
				if let Some(e) = e {
					e.drop().await.unwrap()
				}
				(FileType::Symlink, len, ino)
			}
			ItemRef::Unknown(_) => todo!("unknown entry type"),
		};

		drop(self_ino);
		job.reply.entry(&TTL, &self.attr(ino, ty, len, &data), 0)
	}
}
