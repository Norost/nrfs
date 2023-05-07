use super::*;

impl Fs {
	pub async fn read(&self, job: crate::job::Read) {
		let f = match self.ino().get(job.ino).unwrap() {
			Get::Key(Key::File(f)) => self.fs.file(f),
			Get::Key(_) => return job.reply.error(libc::EINVAL),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};

		let mut buf = vec![0; job.size as _];
		let l = f.read(job.offset as _, &mut buf).await.unwrap();
		job.reply.data(&buf[..l]);
	}
}
