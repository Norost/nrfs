use super::*;

#[test]
fn create_file() {
	let fs = new();
	run(&fs, async {
		mkfile(&fs.root_dir(), b"file").await;
	});
}

#[test]
fn create_dir() {
	let fs = new();
	run(&fs, async {
		mkdir(&fs.root_dir(), b"dir").await;
	});
}

#[test]
fn create_sym() {
	let fs = new();
	run(&fs, async {
		mksym(&fs.root_dir(), b"sym").await;
	});
}

#[test]
fn get_file() {
	let fs = new();
	run(&fs, async {
		mkfile(&fs.root_dir(), b"file").await;
		let file = fs.root_dir().search(b"file".into()).await.unwrap().unwrap();
		assert_eq!(file.ty, ItemTy::EmbedFile);
	});
}

#[test]
fn get_dir() {
	let fs = new();
	run(&fs, async {
		mkdir(&fs.root_dir(), b"dir").await;
		let dir = fs.root_dir().search(b"dir".into()).await.unwrap().unwrap();
		assert_eq!(dir.ty, ItemTy::Dir);
	});
}

#[test]
fn get_sym() {
	let fs = new();
	run(&fs, async {
		mksym(&fs.root_dir(), b"sym").await;
		let sym = fs.root_dir().search(b"sym".into()).await.unwrap().unwrap();
		assert_eq!(sym.ty, ItemTy::EmbedSym);
	});
}

#[test]
fn destroy_file() {
	let fs = new();
	run(&fs, async {
		let f = mkfile(&fs.root_dir(), b"file").await;
		fs.root_dir().remove(f.key()).await.unwrap().unwrap();
	});
}

#[test]
fn destroy_large_file() {
	let fs = new();
	run(&fs, async {
		let file = mkfile(&fs.root_dir(), b"file").await;
		// Write at least 64KiB of data so an object is guaranteed to be allocated.
		file.write_grow(0, &[0; 1 << 16]).await.unwrap().unwrap();
		fs.root_dir().remove(file.key()).await.unwrap().unwrap();
	});
}

/// Try destroying an empty directory, which should succeed.
#[test]
fn destroy_empty_dir() {
	let fs = new();
	run(&fs, async {
		let d = mkdir(&fs.root_dir(), b"dir").await;
		fs.root_dir().remove(d.key()).await.unwrap().unwrap();
	});
}

/// Try destroying a non-empty directory, which should fail.
#[test]
fn destroy_nonempty_dir() {
	let fs = new();
	run(&fs, async {
		let dir = mkdir(&fs.root_dir(), b"dir").await;
		mkfile(&dir, b"file").await;
		fs.root_dir().remove(dir.key()).await.unwrap().unwrap_err();
	});
}

#[test]
fn destroy_sym() {
	let fs = new();
	run(&fs, async {
		let s = mksym(&fs.root_dir(), b"sym").await;
		fs.root_dir().remove(s.key()).await.unwrap().unwrap();
	});
}

#[test]
fn transfer_self() {
	let fs = new();
	run(&fs, async {
		let root = fs.root_dir();
		let f = mkfile(&root, b"file").await;
		let g = root
			.transfer(f.key(), &root, b"same_file".into())
			.await
			.unwrap()
			.unwrap();
		root.search(b"file".into())
			.await
			.unwrap()
			.ok_or(())
			.unwrap_err();
		let f = root.search(b"same_file".into()).await.unwrap().unwrap();
		assert_eq!(f.key, g);
		fs.file(f.key)
			.write_grow(0, b"panic in the disco")
			.await
			.unwrap()
			.unwrap();
	});
}

#[test]
fn transfer_other() {
	let fs = new();
	run(&fs, async {
		let root = fs.root_dir();
		let dir = mkdir(&root, b"dir").await;
		let f = mkfile(&root, b"file").await;

		root.transfer(f.key(), &dir, b"same_file".into())
			.await
			.unwrap()
			.unwrap();
		root.search(b"file".into())
			.await
			.unwrap()
			.ok_or(())
			.unwrap_err();
		let f = dir.search(b"same_file".into()).await.unwrap().unwrap();
		fs.file(f.key)
			.write_grow(0, b"panic in the disco")
			.await
			.unwrap()
			.unwrap();
	});
}

#[test]
fn destroy_empty() {
	let fs = new();
	run(&fs, async {
		let root = fs.root_dir();
		let dir = mkdir(&root, b"dir").await;
		let f = mkfile(&dir, b"file").await;
		root.remove(dir.key()).await.unwrap().unwrap_err();
		dir.remove(f.key()).await.unwrap().unwrap();
		root.remove(dir.key()).await.unwrap().unwrap();
	})
}
