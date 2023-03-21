use super::*;

impl Fs {
	pub async fn readlink(&self, job: crate::job::ReadLink) {
		let mut buf = [0; 1 << 15];
		let f = self.ino().get_sym(job.ino);
		let f = self.fs.file(f);
		let l = f.read(0, &mut buf).await.unwrap();
		job.reply.data(&buf[..l]);
	}
}
