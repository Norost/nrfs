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
		.unwrap();

		let file = root
			.find(b"This is a string with len >= 14".into())
			.await
			.unwrap()
			.unwrap();
		assert!(matches!(file, Entry::File(_)));
	})
}
