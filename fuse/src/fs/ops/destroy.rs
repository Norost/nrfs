use super::*;

impl Fs {
	pub async fn destroy(&self) {
		self.fs.finish_transaction().await.unwrap();
	}
}
