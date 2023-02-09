use super::*;

impl Fs {
	pub async fn setattr(&self, job: crate::job::SetAttr) {
		let self_ino = self.ino.borrow_mut();

		// Get entry
		let e = self_ino.get(&self.fs, job.ino);

		// Set size, if possible
		let (ty, size) = match &*e {
			ItemRef::Dir(d) => (FileType::Directory, d.len().await.unwrap().into()),
			ItemRef::File(f) => {
				let len = if let Some(size) = job.size {
					f.resize(size).await.unwrap();
					size
				} else {
					f.len().await.unwrap()
				};
				(FileType::RegularFile, len)
			}
			ItemRef::Sym(f) => {
				let len = if let Some(size) = job.size {
					f.resize(size).await.unwrap();
					size
				} else {
					f.len().await.unwrap()
				};
				(FileType::Symlink, len)
			}
			ItemRef::Unknown(_) => unreachable!(),
		};

		// Set extension data
		let mut data = e.data().await.unwrap();

		if let Some(ext) = &mut data.ext_unix {
			if job.mode.is_some() || job.uid.is_some() || job.gid.is_some() {
				job.mode.map(|m| ext.permissions = m as u16 & 0o777);
				job.uid.map(|u| ext.set_uid(u));
				job.gid.map(|g| ext.set_gid(g));
				e.set_ext_unix(ext).await.unwrap();
			}
		}

		if let Some(ext) = &mut data.ext_mtime {
			if let Some(mtime) = job.mtime {
				*ext = match mtime {
					TimeOrNow::Now => mtime_now(),
					TimeOrNow::SpecificTime(t) => mtime_sys(t),
				};
				e.set_ext_mtime(ext).await.unwrap();
			}
		}

		drop(self_ino);
		job.reply.attr(&TTL, &self.attr(job.ino, ty, size, &data));
	}
}
