use {super::*, crate::dir::RemoveError};

#[test]
fn get_root() {
	run(async {
		let fs = new().await;
		fs.root_dir().await.unwrap();
	})
}

#[test]
fn create_file() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();
	})
}

#[test]
fn create_dir() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_dir(
			b"dir".into(),
			&DirOptions::new(&[0; 16]),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap();
	})
}

#[test]
fn create_sym() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_sym(b"sym".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();
	})
}

#[test]
fn create_file_long_name() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_file(
			b"This is a string with len >= 14".into(),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap();
	})
}

#[test]
fn get_file() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();

		let file = root.find(b"file".into()).await.unwrap().unwrap();
		assert!(matches!(file, Entry::File(_)));
	})
}

#[test]
fn get_dir() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_dir(
			b"dir".into(),
			&DirOptions::new(&[0; 16]),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap();

		let dir = root.find(b"dir".into()).await.unwrap().unwrap();
		assert!(matches!(dir, Entry::Dir(_)));
	})
}

#[test]
fn get_sym() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_sym(b"sym".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();

		let sym = root.find(b"sym".into()).await.unwrap().unwrap();
		assert!(matches!(sym, Entry::Sym(_)));
	})
}

#[test]
fn get_file_long_name() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_file(
			b"This is a string with len >= 14".into(),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap();

		let file = root
			.find(b"This is a string with len >= 14".into())
			.await
			.unwrap()
			.unwrap();
		assert!(matches!(file, Entry::File(_)));
	})
}

#[test]
fn remove_file() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();

		root.remove(b"file".into()).await.unwrap().unwrap();
	})
}

#[test]
fn remove_large_file() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		let file = root
			.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();

		// Write at least 64KiB of data so an object is guaranteed to be allocated.
		file.write_grow(0, &[0; 1 << 16]).await.unwrap();
		drop(file);

		root.remove(b"file".into()).await.unwrap().unwrap();
	})
}

/// Try removing an empty directory, which should succeed.
#[test]
fn remove_empty_dir() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_dir(
			b"dir".into(),
			&DirOptions::new(&[0; 16]),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap();

		root.remove(b"dir".into()).await.unwrap().unwrap();
	})
}

/// Try removing an non-empty directory, which should fail.
#[test]
fn remove_nonempty_dir() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_dir(
			b"dir".into(),
			&DirOptions::new(&[0; 16]),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap()
		.create_file(b"file".into(), &Default::default())
		.await
		.unwrap()
		.unwrap();

		assert!(matches!(
			root.remove(b"dir".into()).await.unwrap(),
			Err(RemoveError::NotEmpty)
		));
	})
}

#[test]
fn remove_sym() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_sym(b"sym".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();

		root.remove(b"sym".into()).await.unwrap().unwrap();
	})
}

#[test]
fn remove_file_long_name() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		root.create_file(
			b"This is a string with len >= 14".into(),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap();

		root.remove(b"This is a string with len >= 14".into())
			.await
			.unwrap()
			.unwrap();
	})
}

#[test]
fn rename() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();

		let file = root
			.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();

		let moved = root
			.rename(b"file".into(), b"same_file".into())
			.await
			.unwrap();
		assert!(moved);

		// Check if the associated FileData is still correct.
		file.write_grow(0, b"panic in the disco").await.unwrap();
	})
}

#[test]
fn transfer() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();

		let dir = root
			.create_dir(
				b"dir".into(),
				&DirOptions::new(&[0; 16]),
				&Default::default(),
			)
			.await
			.unwrap()
			.unwrap();

		let file = root
			.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();

		let moved = root
			.transfer(b"file".into(), &dir, b"same_file".into())
			.await
			.unwrap();
		assert!(moved);

		// Check if the associated FileData is still correct.
		file.write_grow(0, b"panic in the disco").await.unwrap();
	})
}

/// Get a directory while another [`DirRef`] pointing to the same directory is alive.
#[test]
fn get_dir_existing_ref() {
	run(async {
		let fs = new().await;
		let root = fs.root_dir().await.unwrap();
		let _dir = root
			.create_dir(
				b"dir".into(),
				&DirOptions::new(&[0; 16]),
				&Default::default(),
			)
			.await
			.unwrap()
			.unwrap();

		let dir2 = root.find(b"dir".into()).await.unwrap().unwrap();
		assert!(matches!(dir2, Entry::Dir(_)));
	})
}
