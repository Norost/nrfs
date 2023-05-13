use super::*;

impl Fs {
	pub async fn fallocate(&self, job: crate::job::FAllocate) {
		let _lock = self.lock_mut(job.ino).await;
		match self.ino().get(job.ino).unwrap() {
			Get::Key(Key::Dir(_), ..) => job.reply.error(libc::EISDIR),
			Get::Key(Key::File(f), ..) => {
				let f = self.fs.file(f);
				if f.resize(job.length as _).await.unwrap().is_err() {
					job.reply.error(libc::EFBIG)
				} else {
					job.reply.ok()
				}
			}
			// TODO which error should we return?
			Get::Key(Key::Sym(_), ..) => job.reply.error(libc::EINVAL),
			Get::Stale => job.reply.error(libc::ESTALE),
		}
	}
}
