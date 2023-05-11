use super::*;

impl Fs {
	pub async fn getxattr(&self, job: crate::job::GetXAttr) {
		if filter_xattr(job.name.as_bytes()) {
			return job.reply.error(libc::EPERM);
		}
		let Ok(name) = job.name.as_bytes().try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };
		let key = match self.ino().get(job.ino).unwrap() {
			Get::Key(k) => *k.key(),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		if let Some(val) = self.fs.item(key).attr(name).await.unwrap() {
			let len = val.len().try_into().unwrap();
			if job.size == 0 {
				job.reply.size(len);
			} else if job.size < len {
				job.reply.error(libc::ERANGE);
			} else {
				job.reply.data(&val);
			}
		} else {
			job.reply.error(libc::ENODATA);
		}
	}
}
