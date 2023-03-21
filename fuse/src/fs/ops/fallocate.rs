use super::*;

impl Fs {
	pub async fn fallocate(&self, job: crate::job::FAllocate) {
		match self.ino().get(job.ino) {
			ItemKey::Dir(_) => job.reply.error(libc::EISDIR),
			ItemKey::File(f) => {
				if self
					.fs
					.file(f)
					.resize(job.length as _)
					.await
					.unwrap()
					.is_err()
				{
					job.reply.error(libc::EFBIG)
				} else {
					job.reply.ok()
				}
			}
			// TODO which error should we return?
			ItemKey::Sym(_) => job.reply.error(libc::EINVAL),
		}
	}
}
