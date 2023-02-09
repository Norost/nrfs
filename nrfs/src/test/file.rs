use super::*;

/// Transfer a file with embedded data and ensure the *heap* data is also moved.
#[test]
fn transfer_embed() {
	let fs = new();
	run(&fs, async {
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

		file.write_grow(0, b"Hello!").await.unwrap();

		root.transfer(b"file".into(), &dir, b"file".into())
			.await
			.unwrap()
			.unwrap();

		// If the embedded data hasn't been transferred, this will crash.
		let buf = &mut [0; 6];
		file.read_exact(0, buf).await.unwrap();
		assert_eq!(*buf, *b"Hello!");

		file.drop().await.unwrap();
		dir.drop().await.unwrap();
		root.drop().await.unwrap();
	});
}
