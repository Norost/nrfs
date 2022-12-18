use super::*;

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

		assert!(root.remove(b"file".into()).await.unwrap());
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

		assert!(root.remove(b"file".into()).await.unwrap());
	})
}

#[test]
fn remove_dir() {
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

		assert!(root.remove(b"dir".into()).await.unwrap());
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

		assert!(root.remove(b"sym".into()).await.unwrap());
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

		assert!(root
			.remove(b"This is a string with len >= 14".into())
			.await
			.unwrap());
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
