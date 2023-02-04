use super::*;

impl Fs {
	pub async fn write(&self, job: crate::job::Write) {
		let self_ino = self.ino.borrow_mut();

		let f = self_ino.get_file(&self.fs, job.ino);
		f.write_grow(job.offset as _, &job.data).await.unwrap();
		job.reply.written(job.data.len() as _);
	}
}
