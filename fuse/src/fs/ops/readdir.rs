use {super::*, std::ffi::OsStr};

impl Fs {
	pub async fn readdir(&self, mut job: crate::job::ReadDir) {
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

		let d = self.ino().get_dir(job.ino);
		let d = self.fs.dir(d);

		let mut index = u32::try_from(job.offset - 2).unwrap_or(u32::MAX);
		while let Some((item, i)) = d.next_from(index).await.unwrap() {
			let Some(name) = &item.name else {
				self.clean_dangling(item.key()).await;
				index = i;
				continue;
			};

			let ty = match item.key() {
				ItemKey::Dir(_) => FileType::Directory,
				ItemKey::File(_) => FileType::RegularFile,
				ItemKey::Sym(_) => FileType::Symlink,
			};

			let offt = i as i64 + 2;
			// It's possible the ino is not known due to readdir not doing an implicit lookup
			// and hence not increasing refcount, which in turns means there may be no entry
			// in the inode store.
			//
			// For consistency's sake, always use NO_INO (-1).
			if job.reply.add(NO_INO, offt, ty, OsStr::from_bytes(&name)) {
				break;
			}
			index = i;
		}

		job.reply.ok();
	}
}
