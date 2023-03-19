use super::*;

impl Fs {
	pub async fn write(&self, job: crate::job::Write) {
		let f = self.ino().get_file(job.ino);
		let f = self.fs.file(f);
		f.write_grow(job.offset as _, &job.data)
			.await
			.unwrap()
			.unwrap();
		job.reply.written(job.data.len() as _);
	}
}
