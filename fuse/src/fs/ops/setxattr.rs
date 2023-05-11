use super::*;

impl Fs {
	pub async fn setxattr(&self, job: crate::job::SetXAttr) {
		if filter_xattr(job.name.as_bytes()) {
			return job.reply.error(libc::EPERM);
		}
		let Ok(name) = job.name.as_bytes().try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };
		let key = match self.ino().get(job.ino).unwrap() {
			Get::Key(k) => *k.key(),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let item = self.fs.item(key);
		if job.flags & libc::XATTR_CREATE != 0 {
			if item.attr(name).await.unwrap().is_some() {
				return job.reply.error(libc::EEXIST);
			}
		}
		if job.flags & libc::XATTR_REPLACE != 0 {
			if item.attr(name).await.unwrap().is_none() {
				return job.reply.error(libc::ENODATA);
			}
		}
		match item.set_attr(name, &job.value).await.unwrap() {
			Ok(()) => job.reply.ok(),
			Err(nrfs::SetAttrError::Full) | Err(nrfs::SetAttrError::IsRoot) => {
				job.reply.error(libc::ENOSPC)
			}
		}
	}
}
