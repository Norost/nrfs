use super::*;

impl Fs {
	pub async fn write(&self, job: crate::job::Write) {
		let f = match self.ino().get(job.ino).unwrap() {
			Get::Key(Key::File(f)) => self.fs.file(f),
			Get::Key(_) => return job.reply.error(libc::EINVAL),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};

		f.write_grow(job.offset as _, &job.data)
			.await
			.unwrap()
			.unwrap();
		job.reply.written(job.data.len() as _);
	}
}
