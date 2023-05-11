use super::*;

impl Fs {
	pub async fn setattr(&self, job: crate::job::SetAttr) {
		let key = match self.ino().get(job.ino).unwrap() {
			Get::Key(k, ..) => k,
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let item = self.fs.item(*key.key());
		let size = item.len().await.unwrap();
		let ty = match key {
			Key::Dir(_) => FileType::Directory,
			Key::File(_) => FileType::RegularFile,
			Key::Sym(_) => FileType::Symlink,
		};

		if let Some(t) = job.mtime {
			let t = match t {
				TimeOrNow::Now => mtime_now(),
				TimeOrNow::SpecificTime(t) => mtime_sys(t),
			};
			set_mtime(&item, t).await
		}
		if let Some(uid) = job.uid {
			set_uid(&item, uid).await
		}
		if let Some(gid) = job.gid {
			set_gid(&item, gid).await
		}
		if let Some(mode) = job.mode {
			set_mode(&item, (mode & 0o777) as _).await
		}

		let attrs = get_attrs(&item).await;
		job.reply.attr(&TTL, &self.attr(job.ino, ty, size, attrs));
		self.update_gen(job.ino).await;
	}
}
