use super::*;

impl Fs {
	pub async fn getxattr(&self, job: crate::job::GetXAttr) {
		if &*job.name != b"nrfs.gen" && filter_xattr(&job.name) {
			return job.reply.error(libc::EPERM);
		}
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };
		let key = match self.ino().get(job.ino).unwrap() {
			Get::Key(k, ..) => *k.key(),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let item = self.fs.item(key);
		let (a, b);
		let val = if &*job.name == b"nrfs.gen" {
			a = item.modified().await.unwrap().gen.to_le_bytes();
			&a
		} else if let Some(v) = item.attr(name).await.unwrap() {
			b = v;
			&b[..]
		} else {
			return job.reply.error(libc::ENODATA);
		};
		let len = val.len().try_into().unwrap();
		if job.size == 0 {
			job.reply.size(len);
		} else if job.size < len {
			job.reply.error(libc::ERANGE);
		} else {
			job.reply.data(&val);
		}
	}
}
