use super::*;

impl Fs {
	pub async fn forget(&self, job: crate::job::Forget) {
		self.ino().forget(job.ino, job.nlookup);
	}
}
