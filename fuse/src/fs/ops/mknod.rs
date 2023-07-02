use super::*;

impl Fs {
	pub async fn mknod(&self, job: crate::job::MkNod) {
		let Ok(name) = (&*job.name).try_into()
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let ty = match job.mode & libc::S_IFMT {
			libc::S_IFREG => TY_BUILTIN,
			libc::S_IFCHR => TY_CHAR,
			libc::S_IFBLK => TY_BLOCK,
			libc::S_IFIFO => TY_PIPE,
			libc::S_IFSOCK => TY_SOCK,
			_ => return job.reply.error(libc::EINVAL),
		};

		let (dir, lock) = match self.dir_mut(job.parent).await {
			Ok(r) => r,
			Err(e) => return job.reply.error(e),
		};

		match dir.create_file(name).await.unwrap() {
			Ok(f) => {
				let attrs = self
					.init_attrs(&f, job.uid, job.gid, Some(job.mode as u16 & 0o777 | ty))
					.await;
				let ino = self.ino().add(Key::File(f.key()), job.parent, self.gen());
				f.set_modified_gen(self.gen()).await.unwrap();
				let attr = self.attr(ino, getty(ty).unwrap(), 0, attrs);
				job.reply.entry(&TTL, &attr, 0);
				self.update_gen(job.parent, lock).await;
			}
			Err(CreateError::Duplicate) => job.reply.error(libc::EEXIST),
			Err(CreateError::Full) => job.reply.error(libc::ENOSPC),
		}
	}
}
