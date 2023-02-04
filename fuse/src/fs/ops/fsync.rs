use super::*;

impl Fs {
	pub async fn fsync(&self, job: crate::job::FSync) {
		self.fs.finish_transaction().await.unwrap();
		job.reply.ok();
	}
}
