use super::*;

impl Fs {
	pub async fn setattr(&self, job: crate::job::SetAttr) {
		let item = self.ino().get(job.ino);
		let item = self.fs.item(item);

		// Set size, if possible
		let (ty, size) = match item.key() {
			ItemKey::Dir(_) => (FileType::Directory, 0),
			ItemKey::File(f) => {
				let f = self.fs.file(f);
				let len = if let Some(size) = job.size {
					f.resize(size).await.unwrap().unwrap();
					size
				} else {
					f.len().await.unwrap()
				};
				(FileType::RegularFile, len)
			}
			ItemKey::Sym(f) => {
				let len = self.fs.file(f).len().await.unwrap();
				(FileType::Symlink, len)
			}
		};

		let mut ext = item.ext().await.unwrap();
		if let Some(ext) = &mut ext.unix {
			if job.mode.is_some() || job.uid.is_some() || job.gid.is_some() {
				job.mode.map(|m| ext.permissions = m as u16 & 0o777);
				job.uid.map(|u| ext.set_uid(u));
				job.gid.map(|g| ext.set_gid(g));
				item.set_unix(ext.clone()).await.unwrap();
			}
		}
		if let Some(ext) = &mut ext.mtime {
			if let Some(mtime) = job.mtime {
				*ext = match mtime {
					TimeOrNow::Now => mtime_now(),
					TimeOrNow::SpecificTime(t) => mtime_sys(t),
				};
				item.set_mtime(ext.clone()).await.unwrap();
			}
		}

		job.reply.attr(&TTL, &self.attr(job.ino, ty, size, ext));
	}
}
