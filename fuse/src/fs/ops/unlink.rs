use super::*;

impl Fs {
	pub async fn unlink<'a>(&'a self, bg: &Background<'a, FileDev>, job: crate::job::Unlink) {
		let Ok(name) = job.name.as_bytes().try_into() else { return job.reply.error(libc::ENAMETOOLONG) };
		match self.remove_file(bg, job.parent, name).await {
			Ok(()) => job.reply.ok(),
			Err(e) => job.reply.error(e),
		}
	}
}
