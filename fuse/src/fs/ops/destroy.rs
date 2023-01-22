use super::*;

impl Fs {
	pub async fn destroy<'a>(&'a self, bg: &Background<'a, FileDev>) {
		let mut self_ino = self.ino.borrow_mut();

		self_ino.remove_all(&self.fs, bg).await;
		self.fs.finish_transaction(bg).await.unwrap();
	}
}
