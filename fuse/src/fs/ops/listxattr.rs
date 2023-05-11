use super::*;

impl Fs {
	pub async fn listxattr(&self, job: crate::job::ListXAttr) {
		let key = match self.ino().get(job.ino).unwrap() {
			Get::Key(k, ..) => *k.key(),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let item = self.fs.item(key);
		let keys = item.attr_keys().await.unwrap();
		let it = || {
			keys.iter()
				.filter(|k| !filter_xattr(&**k))
				.map(|k| &***k)
				.chain([&b"nrfs.gen"[..]])
		};
		let len = it().fold(0, |s, k| s + k.len() + 1).try_into().unwrap();
		if job.size == 0 {
			job.reply.size(len);
		} else if job.size < len {
			job.reply.error(libc::ERANGE);
		} else {
			let val = it()
				.flat_map(|k| [k, b"\0"])
				.flat_map(|k| k)
				.copied()
				.collect::<Vec<_>>();
			job.reply.data(&val);
		}
	}
}
