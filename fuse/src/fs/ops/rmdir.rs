use super::*;

impl Fs {
	pub async fn rmdir(&self, job: crate::job::RmDir) {
		let Ok(name) = job.name.as_bytes().try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let parent = self.ino().get_dir(job.parent);
		let parent = self.fs.dir(parent);

		let Some(item) = parent.search(name).await.unwrap()
			else { return job.reply.error(libc::ENOENT) };
		let ItemKey::Dir(dir) = item.key()
			else { return job.reply.error(libc::ENOTDIR) };
		let dir = self.fs.dir(dir);

		if !dir_is_empty(&dir).await {
			return job.reply.error(libc::ENOTEMPTY);
		}

		let ino = self.ino().get_ino(item.key());
		if let Some(ino) = ino {
			self.fs.item(item.key()).erase_name().await.unwrap();
			self.ino().mark_unlinked(ino);
		} else {
			match dir.destroy().await.unwrap() {
				Ok(()) => {}
				Err(DirDestroyError::NotEmpty) => {
					self.fs.item(item.key()).erase_name().await.unwrap()
				}
				Err(DirDestroyError::IsRoot) => return job.reply.error(libc::EBUSY),
			}
		}
		job.reply.ok();
	}
}
