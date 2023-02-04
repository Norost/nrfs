use super::*;

impl Fs {
	pub async fn read(&self, job: crate::job::Read) {
		let self_ino = self.ino.borrow_mut();

		let mut buf = vec![0; job.size as _];
		let f = self_ino.get_file(&self.fs, job.ino);
		let l = f.read(job.offset as _, &mut buf).await.unwrap();
		job.reply.data(&buf[..l]);
	}
}
