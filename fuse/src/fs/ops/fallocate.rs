use super::*;

impl Fs {
	pub async fn fallocate<'a>(&'a self, bg: &Background<'a, FileDev>, job: crate::job::FAllocate) {
		let self_ino = self.ino.borrow_mut();

		match &*self_ino.get(&self.fs, bg, job.ino) {
			ItemRef::Dir(_) => job.reply.error(libc::EISDIR),
			ItemRef::File(f) => f.resize(job.length as _).await.unwrap(),
			ItemRef::Sym(f) => f.resize(job.length as _).await.unwrap(),
			ItemRef::Unknown(_) => unreachable!(),
		}
	}
}
