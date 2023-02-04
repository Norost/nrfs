use super::*;

impl Fs {
	pub async fn forget(&self, job: crate::job::Forget) {
		let mut self_ino = self.ino.borrow_mut();

		if let Some(r) = self_ino.forget(&self.fs, job.ino, job.nlookup) {
			r.drop().await.unwrap();
		}
	}
}
