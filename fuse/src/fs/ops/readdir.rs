use {super::*, std::ffi::OsStr};

impl Fs {
	pub async fn readdir<'a>(&'a self, bg: &Background<'a, FileDev>, mut job: crate::job::ReadDir) {
		let mut self_ino = self.ino.borrow_mut();

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

		let mut d = self_ino.get_dir(&self.fs, bg, job.ino);

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

			let (ty, e_ino) = match e {
				ItemRef::Dir(d) => {
					let (ino, e) = self_ino.add_dir(d, false);
					if let Some(e) = e {
						e.drop().await.unwrap()
					}
					(FileType::Directory, ino)
				}
				ItemRef::File(f) => {
					let (ino, e) = self_ino.add_file(f, false);
					if let Some(e) = e {
						e.drop().await.unwrap()
					}
					(FileType::RegularFile, ino)
				}
				ItemRef::Sym(f) => {
					let (ino, e) = self_ino.add_sym(f, false);
					if let Some(e) = e {
						e.drop().await.unwrap()
					}
					(FileType::Symlink, ino)
				}
				ItemRef::Unknown(_) => todo!("miscellaneous file type"),
			};
			d = self_ino.get_dir(&self.fs, bg, job.ino);

			let offt = i as i64 + 2;
			if job.reply.add(e_ino, offt, ty, OsStr::from_bytes(&name)) {
				break;
			}
			index = i;
		}

		job.reply.ok();
	}
}
