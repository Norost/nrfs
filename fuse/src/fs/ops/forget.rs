use super::*;

impl Fs {
	pub async fn forget(&self, job: crate::job::Forget) {
		let res = self.ino().forget(job.ino, job.nlookup);
		let Some((key, dangling)) = res else { return };
		if !dangling {
			return;
		}
		match key {
			ItemKey::Dir(d) => match self.fs.dir(d).destroy().await.unwrap() {
				Ok(()) => {}
				// May happen if there are dangling items inside the dir.
				// It'll get cleaned up later, probably.
				Err(DirDestroyError::NotEmpty) => {}
				Err(DirDestroyError::IsRoot) => unreachable!(),
			},
			ItemKey::File(f) | ItemKey::Sym(f) => self.fs.file(f).destroy().await.unwrap(),
		}
	}
}
