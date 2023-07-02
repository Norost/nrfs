use super::*;

impl Fs {
	pub async fn setattr(&self, job: crate::job::SetAttr) {
		let lock = self.lock_mut(job.ino).await;
		let key = match self.ino().get(job.ino).unwrap() {
			Get::Key(k, ..) => k,
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let item = self.fs.item(*key.key());
		let size = item.len().await.unwrap();

		let mut attrs = get_attrs(&item).await;

		let ty = match key {
			Key::Dir(_) => FileType::Directory,
			Key::Sym(_) => FileType::Symlink,
			Key::File(_) => getty(attrs.mode.unwrap_or(0)).unwrap_or(FileType::RegularFile),
		};

		if let Some(t) = job.mtime {
			let t = match t {
				TimeOrNow::Now => mtime_now(),
				TimeOrNow::SpecificTime(t) => mtime_sys(t),
			};
			attrs.modified.time = t;
			set_mtime(&item, t).await
		}
		if let Some(uid) = job.uid {
			attrs.gid = Some(uid);
			set_uid(&item, uid).await
		}
		if let Some(gid) = job.gid {
			attrs.gid = Some(gid);
			set_gid(&item, gid).await
		}
		if let Some(mode) = job.mode {
			let mode = (mode as u16 & 0o777) | (attrs.mode.unwrap_or(0) & 0o7_000);
			attrs.mode = Some(mode);
			set_mode(&item, mode).await
		}

		job.reply.attr(&TTL, &self.attr(job.ino, ty, size, attrs));
		self.update_gen(job.ino, lock).await;
	}
}
