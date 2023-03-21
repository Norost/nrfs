use super::*;

impl Fs {
	pub async fn read(&self, job: crate::job::Read) {
		let mut buf = vec![0; job.size as _];
		let f = self.ino().get_file(job.ino);
		let f = self.fs.file(f);
		let l = f.read(job.offset as _, &mut buf).await.unwrap();
		job.reply.data(&buf[..l]);
	}
}
