use {super::*, std::ffi::OsStr};

impl Fs {
	pub async fn readdir(&self, mut job: crate::job::ReadDir) {
		let self_ino = self.ino.borrow_mut();

		if job.offset == 0 {
			if job.reply.add(job.ino, 1, FileType::Directory, ".") {
				return job.reply.ok();
			}
			job.offset += 1;
		}

		if job.offset == 1 {
			if job.reply.add(job.ino, 2, FileType::Directory, "..") {
				return job.reply.ok();
			}
			job.offset += 1;
		}

		let mut d = self_ino.get_dir(&self.fs, job.ino);

		let mut index = job.offset as u64 - 2;
		while let Some((e, i)) = d.next_from(index).await.unwrap() {
			let data = e.data().await.unwrap();
			let Some(name) = e.key(&data).await.unwrap() else {
					// Entry may have been removed just after we fetched it,
					// so just skip.
					e.drop().await.unwrap();
					index = i;
					continue;
				};

			// It's possible the ino is not known due to readdir not doing an implicit lookup
			// and hence not increasing refcount, which in turns means there may be no entry
			// in the inode store.
			//
			// For consistency's sake, always use NO_INO (-1).
			let ty = match &e {
				ItemRef::Dir(_) => FileType::Directory,
				ItemRef::File(_) => FileType::RegularFile,
				ItemRef::Sym(_) => FileType::Symlink,
				ItemRef::Unknown(_) => todo!("miscellaneous file type"),
			};
			e.drop().await.unwrap();
			d = self_ino.get_dir(&self.fs, job.ino);

			let offt = i as i64 + 2;
			if job.reply.add(NO_INO, offt, ty, OsStr::from_bytes(&name)) {
				break;
			}
			index = i;
		}

		job.reply.ok();
	}
}
