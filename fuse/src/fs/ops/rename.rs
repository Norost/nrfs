use super::*;

impl Fs {
	pub async fn rename(&self, job: crate::job::Rename) {
		let (Ok(from_name), Ok(to_name)) = ((&*job.name).try_into(), (&*job.newname).try_into())
			else { return job.reply.error(libc::ENAMETOOLONG) };

		let lock_a = self.lock_mut(job.parent.min(job.newparent)).await;
		let lock_b = if job.parent != job.newparent {
			Some(self.lock_mut(job.parent.max(job.newparent)).await)
		} else {
			None
		};
		let from_d = match self.ino().get(job.parent).unwrap() {
			Get::Key(Key::Dir(d), ..) => self.fs.dir(d),
			Get::Key(..) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let from_d = from_d.await.unwrap();
		let to_d = match self.ino().get(job.newparent).unwrap() {
			Get::Key(Key::Dir(d), ..) => self.fs.dir(d),
			Get::Key(..) => return job.reply.error(libc::ENOTDIR),
			Get::Stale => return job.reply.error(libc::ESTALE),
		};
		let to_d = to_d.await.unwrap();

		let Some(from_item) = from_d.search(from_name).await.unwrap()
			else { return job.reply.error(libc::ENOENT) };
		// FIXME potential deadlock
		let _lock_x = self
			.ino()
			.get_ino(from_item.key)
			.map(|ino| self.lock_mut(ino));
		let _lock_x = if let Some(task) = _lock_x {
			Some(task.await)
		} else {
			None
		};

		if let Some(to_item) = to_d.search(to_name).await.unwrap() {
			let ino = self.ino().get_ino(to_item.key);
			// FIXME potential deadlock
			let _lock_y = if let Some(ino) = ino {
				Some(self.lock_mut(ino).await)
			} else {
				None
			};
			match to_d.remove(to_item.key).await.unwrap() {
				Ok(()) => {}
				Err(nrfs::RemoveError::NotEmpty) => return job.reply.error(libc::ENOTEMPTY),
			}
			if let Some(ino) = ino {
				self.ino().mark_stale(ino);
			}
		}

		match from_d
			.transfer(from_item.key, &to_d, to_name)
			.await
			.unwrap()
		{
			Ok(key) => {
				let ino = self.ino().get_ino(from_item.key);
				ino.map(|ino| self.ino().set(ino, key, job.newparent));
				job.reply.ok();
				self.fs
					.item(key)
					.set_modified_gen(self.gen())
					.await
					.unwrap();
				self.update_gen(job.parent, lock_a).await;
				if let Some(lock_b) = lock_b {
					self.update_gen(job.newparent, lock_b).await;
				}
			}
			Err(nrfs::TransferError::Duplicate) => unreachable!(),
			Err(nrfs::TransferError::Full) => todo!(),
		}
	}
}
