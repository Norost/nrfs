use super::*;

impl Fs {
	pub async fn readlink<'a>(&'a self, bg: &Background<'a, FileDev>, job: crate::job::ReadLink) {
		let self_ino = self.ino.borrow_mut();

		let mut buf = [0; 1 << 15];
		let f = self_ino.get_sym(&self.fs, bg, job.ino);
		let l = f.read(0, &mut buf).await.unwrap();
		job.reply.data(&buf[..l]);
	}
}
