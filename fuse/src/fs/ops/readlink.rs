use super::*;

impl Fs {
	pub async fn readlink(&self, job: crate::job::ReadLink) {
		let _lock = self.lock(job.ino).await;
		let f = match self.ino().get(job.ino).unwrap() {
			Get::Key(Key::Sym(f), ..) => self.fs.file(f),
			Get::Key(..) => return job.reply.error(libc::EINVAL),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};

		let mut buf = [0; 1 << 15];
		let l = f.read(0, &mut buf).await.unwrap();
		job.reply.data(&buf[..l]);
	}
}
