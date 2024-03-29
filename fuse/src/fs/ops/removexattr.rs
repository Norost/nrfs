use super::*;

impl Fs {
	pub async fn removexattr(&self, job: crate::job::RemoveXAttr) {
		if filter_xattr(&job.name) {
			return job.reply.error(libc::EPERM);
		}
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };
		let lock = self.lock_mut(job.ino).await;
		let key = match self.ino().get(job.ino).unwrap() {
			Get::Key(k, ..) => *k.key(),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		if self.fs.item(key).del_attr(name).await.unwrap() {
			job.reply.ok();
			self.update_gen(job.ino, lock).await;
		} else {
			job.reply.error(libc::ENODATA);
		}
	}
}
