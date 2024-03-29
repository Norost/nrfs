use {super::*, nrfs::ItemTy, std::ffi::OsStr};

impl Fs {
	pub async fn readdir(&self, mut job: crate::job::ReadDir) {
		if job.offset == 0 {
			if job.reply.add(job.ino, 1, FileType::Directory, ".") {
				return job.reply.ok();
			}
			job.offset += 1;
		}

		if job.offset == 1 {
			let Get::Key(_, parent_ino, _) = self.ino().get(job.ino).unwrap() else { unreachable!() };
			if job.reply.add(parent_ino, 2, FileType::Directory, "..") {
				return job.reply.ok();
			}
			job.offset += 1;
		}

		let (dir, _lock) = match self.dir(job.ino).await {
			Ok(r) => r,
			Err(e) => return job.reply.error(e),
		};

		let mut index = job.offset as u64 - 2;
		while let Some((item, i)) = dir.next_from(index).await.unwrap() {
			let ty = match item.ty {
				ItemTy::Dir => FileType::Directory,
				ItemTy::File | ItemTy::EmbedFile => FileType::RegularFile,
				ItemTy::Sym | ItemTy::EmbedSym => FileType::Symlink,
			};

			let offt = (i + 2) as i64;
			// It's possible the ino is not known due to readdir not doing an implicit lookup
			// and hence not increasing refcount, which in turns means there may be no entry
			// in the inode store.
			//
			// For consistency's sake, always use NO_INO (-1).
			if job
				.reply
				.add(NO_INO, offt, ty, OsStr::from_bytes(&item.name))
			{
				break;
			}
			index = i;
		}

		job.reply.ok();
	}
}
