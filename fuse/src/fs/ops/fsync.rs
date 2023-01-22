use super::*;

impl Fs {
	pub async fn fsync<'a, 'b>(&'a self, bg: &'b Background<'a, FileDev>, job: crate::job::FSync) {
		self.fs.finish_transaction(bg).await.unwrap();
		job.reply.ok();
	}
}
