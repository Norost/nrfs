use super::*;

const BUMP_GEN: u32 = 1;

impl Fs {
	pub async fn ioctl(&self, job: crate::job::IoCtl) {
		match job.cmd {
			BUMP_GEN => {
				self.next_gen();
				job.reply.ioctl(0, &[]);
			}
			_ => job.reply.error(libc::EINVAL),
		}
	}
}
