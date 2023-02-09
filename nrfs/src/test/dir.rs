use {super::*, crate::dir::RemoveError};

#[test]
fn get_root() {
	let fs = new();

	run(&fs, async {
		fs.root_dir().await.unwrap().drop().await.unwrap();
	});
}

#[test]
fn create_file() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		root.drop().await.unwrap();
	});
}

#[test]
fn create_dir() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_dir(
			b"dir".into(),
			&DirOptions::new(&[0; 16]),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap()
		.drop()
		.await
		.unwrap();
		root.drop().await.unwrap();
	});
}

#[test]
fn create_sym() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_sym(b"sym".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		root.drop().await.unwrap();
	});
}

#[test]
fn create_file_long_name() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_file(
			b"This is a string with len equal to 37"[..]
				.try_into()
				.unwrap(),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap()
		.drop()
		.await
		.unwrap();
		root.drop().await.unwrap();
	});
}

#[test]
fn get_file() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();

		let file = root.find(b"file".into()).await.unwrap().unwrap();
		assert!(matches!(file, ItemRef::File(_)));
		file.drop().await.unwrap();

		root.drop().await.unwrap();
	});
}

#[test]
fn get_dir() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_dir(
			b"dir".into(),
			&DirOptions::new(&[0; 16]),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap()
		.drop()
		.await
		.unwrap();

		let dir = root.find(b"dir".into()).await.unwrap().unwrap();
		assert!(matches!(dir, ItemRef::Dir(_)));
		dir.drop().await.unwrap();

		root.drop().await.unwrap();
	});
}

#[test]
fn get_sym() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_sym(b"sym".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();

		let sym = root.find(b"sym".into()).await.unwrap().unwrap();
		assert!(matches!(sym, ItemRef::Sym(_)));
		sym.drop().await.unwrap();

		root.drop().await.unwrap();
	});
}

#[test]
fn get_file_long_name() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_file(
			b"This is a string with len >= 27".into(),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap()
		.drop()
		.await
		.unwrap();

		let file = root
			.find(b"This is a string with len >= 27".into())
			.await
			.unwrap()
			.unwrap();
		assert!(matches!(file, ItemRef::File(_)));
		file.drop().await.unwrap();

		root.drop().await.unwrap();
	});
}

#[test]
fn remove_file() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();

		root.remove(b"file".into()).await.unwrap().unwrap();

		root.drop().await.unwrap();
	});
}

#[test]
fn remove_large_file() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		let file = root
			.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();

		// Write at least 64KiB of data so an object is guaranteed to be allocated.
		file.write_grow(0, &[0; 1 << 16]).await.unwrap();
		file.drop().await.unwrap();

		root.remove(b"file".into()).await.unwrap().unwrap();

		root.drop().await.unwrap();
	});
}

/// Try removing an empty directory, which should succeed.
#[test]
fn remove_empty_dir() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_dir(
			b"dir".into(),
			&DirOptions::new(&[0; 16]),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap()
		.drop()
		.await
		.unwrap();

		root.remove(b"dir".into()).await.unwrap().unwrap();

		root.drop().await.unwrap();
	});
}

/// Try removing an non-empty directory, which should fail.
#[test]
fn remove_nonempty_dir() {
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
		dir.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		dir.drop().await.unwrap();

		assert!(matches!(
			root.remove(b"dir".into()).await.unwrap(),
			Err(RemoveError::NotEmpty)
		));

		root.drop().await.unwrap();
	});
}

#[test]
fn remove_sym() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_sym(b"sym".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();

		root.remove(b"sym".into()).await.unwrap().unwrap();

		root.drop().await.unwrap();
	});
}

#[test]
fn remove_file_long_name() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_file(
			b"This is a string with len >= 14".into(),
			&Default::default(),
		)
		.await
		.unwrap()
		.unwrap()
		.drop()
		.await
		.unwrap();

		root.remove(b"This is a string with len >= 14".into())
			.await
			.unwrap()
			.unwrap();

		root.drop().await.unwrap();
	});
}

#[test]
fn rename() {
	let fs = new();

	run(&fs, async {
		let root = fs.root_dir().await.unwrap();

		let file = root
			.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();

		root.rename(b"file".into(), b"same_file".into())
			.await
			.unwrap()
			.unwrap();

		// Check if the associated FileData is still correct.
		file.write_grow(0, b"panic in the disco").await.unwrap();
		file.drop().await.unwrap();

		root.drop().await.unwrap();
	});
}

#[test]
fn transfer() {
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

		root.transfer(b"file".into(), &dir, b"same_file".into())
			.await
			.unwrap()
			.unwrap();

		// Check if the associated FileData is still correct.
		file.write_grow(0, b"panic in the disco").await.unwrap();
		file.drop().await.unwrap();
		dir.drop().await.unwrap();

		root.drop().await.unwrap();
	});
}

/// Get a directory while another [`DirRef`] pointing to the same directory is alive.
#[test]
fn get_dir_existing_ref() {
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

		let dir2 = root.find(b"dir".into()).await.unwrap().unwrap();
		assert!(matches!(dir2, ItemRef::Dir(_)));
		dir2.drop().await.unwrap();
		dir.drop().await.unwrap();

		root.drop().await.unwrap();
	});
}

/// `NotEmpty` & other errors must be returned before `LiveReference` to avoid confusion.
#[test]
fn error_priority() {
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

		let file = dir
			.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();

		let err = root.remove(b"dir".into()).await.unwrap().unwrap_err();
		assert!(matches!(err, RemoveError::NotEmpty));
		file.drop().await.unwrap();
		dir.drop().await.unwrap();

		root.drop().await.unwrap();
	});
}

/// The key is stored in two places: the hashmap entry and the item.
///
/// Ensure that rename updates both.
#[test]
fn rename_item_key() {
	let fs = new();
	run(&fs, async {
		let root = fs.root_dir().await.unwrap();
		root.create_file(b"file".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		root.rename(b"file".into(), b"same_file".into())
			.await
			.unwrap()
			.unwrap();

		let (e, i) = root.next_from(0).await.unwrap().unwrap();
		let data = e.data().await.unwrap();
		let name = e.key(&data).await.unwrap();
		assert_eq!(
			name.as_ref().map(|n| &**n),
			Some(<&Name>::from(b"same_file"))
		);
		assert!(root.next_from(i).await.unwrap().is_none());
		e.drop().await.unwrap();

		root.drop().await.unwrap();
	});
}
