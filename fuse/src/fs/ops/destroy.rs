use super::*;

impl Fs {
	pub async fn destroy(&self) {
		let mut self_ino = self.ino.borrow_mut();

		self_ino.remove_all(&self.fs).await;
		self.fs.finish_transaction().await.unwrap();
	}
}
