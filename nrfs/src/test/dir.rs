use super::*;

#[test]
fn create_file() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_file(b"file".into(), Default::default())
			.await
			.unwrap()
			.unwrap();
	});
}

#[test]
fn create_dir() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_dir(b"dir".into(), Default::default(), Default::default())
			.await
			.unwrap()
			.unwrap();
	});
}

#[test]
fn create_sym() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_sym(b"sym".into(), Default::default())
			.await
			.unwrap()
			.unwrap();
	});
}

#[test]
fn create_file_long_name() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_file(
				b"This is a string with len equal to 37"[..]
					.try_into()
					.unwrap(),
				Default::default(),
			)
			.await
			.unwrap()
			.unwrap();
	});
}

#[test]
fn get_file() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_file(b"file".into(), Default::default())
			.await
			.unwrap()
			.unwrap();
		let file = fs.root_dir().search(b"file".into()).await.unwrap().unwrap();
		assert!(matches!(file.key(), ItemKey::File(_)));
	});
}

#[test]
fn get_dir() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_dir(b"dir".into(), Default::default(), Default::default())
			.await
			.unwrap()
			.unwrap();
		let dir = fs.root_dir().search(b"dir".into()).await.unwrap().unwrap();
		assert!(matches!(dir.key(), ItemKey::Dir(_)));
	});
}

#[test]
fn get_sym() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_sym(b"sym".into(), Default::default())
			.await
			.unwrap()
			.unwrap();
		let sym = fs.root_dir().search(b"sym".into()).await.unwrap().unwrap();
		assert!(matches!(sym.key(), ItemKey::Sym(_)));
	});
}

#[test]
fn get_file_long_name() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_file(
				b"This is a string with len >= 15".into(),
				Default::default(),
			)
			.await
			.unwrap()
			.unwrap();
		let file = fs
			.root_dir()
			.search(b"This is a string with len >= 15".into())
			.await
			.unwrap()
			.unwrap();
		assert!(matches!(file.key(), ItemKey::File(_)));
	});
}

#[test]
fn destroy_file() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_file(b"file".into(), Default::default())
			.await
			.unwrap()
			.unwrap()
			.destroy()
			.await
			.unwrap();
	});
}

#[test]
fn destroy_large_file() {
	let fs = new();
	run(&fs, async {
		let file = fs
			.root_dir()
			.create_file(b"file".into(), Default::default())
			.await
			.unwrap()
			.unwrap();

		// Write at least 64KiB of data so an object is guaranteed to be allocated.
		file.write_grow(0, &[0; 1 << 16]).await.unwrap().unwrap();

		file.destroy().await.unwrap();
	});
}

/// Try destroying an empty directory, which should succeed.
#[test]
fn destroy_empty_dir() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_dir(b"dir".into(), Default::default(), Default::default())
			.await
			.unwrap()
			.unwrap()
			.destroy()
			.await
			.unwrap()
			.unwrap();
	});
}

/// Try destroying a non-empty directory, which should fail.
#[test]
fn destroy_nonempty_dir() {
	let fs = new();
	run(&fs, async {
		let dir = fs
			.root_dir()
			.create_dir(b"dir".into(), Default::default(), Default::default())
			.await
			.unwrap()
			.unwrap();
		dir.create_file(b"file".into(), Default::default())
			.await
			.unwrap()
			.unwrap();
		dir.destroy().await.unwrap().unwrap_err();
	});
}

#[test]
fn destroy_sym() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_sym(b"sym".into(), Default::default())
			.await
			.unwrap()
			.unwrap()
			.destroy()
			.await
			.unwrap();
	});
}

#[test]
fn destroy_file_long_name() {
	let fs = new();
	run(&fs, async {
		fs.root_dir()
			.create_file(
				b"This is a string with len >= 15".into(),
				Default::default(),
			)
			.await
			.unwrap()
			.unwrap()
			.destroy()
			.await
			.unwrap();
	});
}

#[test]
fn transfer_self() {
	let fs = new();
	run(&fs, async {
		let root = fs.root_dir();

		let mut file = root
			.create_file(b"file".into(), Default::default())
			.await
			.unwrap()
			.unwrap();

		file.transfer(&root, b"same_file".into())
			.await
			.unwrap()
			.unwrap();

		// Check if the associated FileData is still correct.
		file.write_grow(0, b"panic in the disco")
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

		let dir = root
			.create_dir(b"dir".into(), Default::default(), Default::default())
			.await
			.unwrap()
			.unwrap();

		let mut file = root
			.create_file(b"file".into(), Default::default())
			.await
			.unwrap()
			.unwrap();

		file.transfer(&dir, b"same_file".into())
			.await
			.unwrap()
			.unwrap();

		// Check if the associated FileData is still correct.
		file.write_grow(0, b"panic in the disco")
			.await
			.unwrap()
			.unwrap();
	});
}

#[test]
fn insert_multiblock_name() {
	let fs = new();
	run(&fs, async {
		let root = fs.root_dir();

		let a = mkfile(&root, b"a", Default::default()).await;
		let _ = mkfile(&root, b"b", Default::default()).await;
		let c = mkfile(&root, b"c", Default::default()).await;
		let _ = mkfile(&root, b"x", Default::default()).await;

		a.destroy().await.unwrap();
		c.destroy().await.unwrap();

		mkfile(&root, &[b'o'; 16], Default::default()).await;

		root.create_file(b"x".into(), Default::default())
			.await
			.unwrap()
			.unwrap_err();
	})
}

#[test]
fn destroy_empty() {
	let fs = new();
	run(&fs, async {
		let root = fs.root_dir();
		let _ = mkfile(&root, b"a", Default::default()).await;
		let b = mkdir(&root, b"b", Default::default()).await;
		let c = mkfile(&b, b"c", Default::default()).await;
		c.destroy().await.unwrap();
		b.destroy().await.unwrap().unwrap();
	})
}

#[test]
fn iter_ext() {
	let fs = new_ext();
	run(&fs, async {
		let root = fs.root_dir();
		mkfile(
			&root,
			b"file",
			ItemExt { mtime: Some(MTime { mtime: 0xdeadbeef }), ..Default::default() },
		)
		.await;
		let (file, _) = root.next_from(0).await.unwrap().unwrap();
		assert_eq!(file.ext.mtime.unwrap().mtime, 0xdeadbeef);
	});
}
