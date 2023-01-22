use super::*;

impl Fs {
	pub async fn read<'a>(&'a self, bg: &Background<'a, FileDev>, job: crate::job::Read) {
		let self_ino = self.ino.borrow_mut();

		let mut buf = vec![0; job.size as _];
		let f = self_ino.get_file(&self.fs, bg, job.ino);
		let l = f.read(job.offset as _, &mut buf).await.unwrap();
		job.reply.data(&buf[..l]);
	}
}
