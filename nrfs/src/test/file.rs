use super::*;

/// Transfer a file with embedded data and ensure the *heap* data is also moved.
#[test]
fn transfer_embed() {
	let fs = new();
	run(&fs, async {
		let root = fs.root_dir();

		let dir = mkdir(&root, b"dir").await;
		let file = mkfile(&root, b"file").await;
		file.write_grow(0, b"Hello!").await.unwrap().unwrap();

		let f = root
			.transfer(file.key(), &dir, b"file".into())
			.await
			.unwrap()
			.unwrap();

		// If the embedded data hasn't been transferred, this will crash.
		let buf = &mut [0; 6];
		let l = fs.file(f).read(0, buf).await.unwrap();
		assert_eq!(l, 6);
		assert_eq!(*buf, *b"Hello!");
	});
}

#[test]
fn reembed_empty() {
	let fs = new();
	run(&fs, async {
		let file = mkfile(&fs.root_dir(), b"file").await;
		file.resize(1 << 20).await.unwrap().unwrap();
		file.resize(0).await.unwrap().unwrap();
	});
}

#[test]
fn writegrow_far() {
	let fs = new();
	run(&fs, async {
		let file = mkfile(&fs.root_dir(), b"file").await;
		file.write_grow(1 << 20, &[1; 256]).await.unwrap().unwrap();
		let mut buf = [2; 256];
		file.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 256]);
	});
}

#[test]
fn resize_verylarge() {
	let fs = new();
	run(&fs, async {
		let file = mkfile(&fs.root_dir(), b"\0").await;
		file.resize(1 << 32).await.unwrap().unwrap_err();
	});
}

#[test]
fn writegrow_veryfar() {
	let fs = new();
	run(&fs, async {
		let file = mkfile(&fs.root_dir(), b"\0").await;
		file.write_grow(1 << 32, &[1; 256])
			.await
			.unwrap()
			.unwrap_err();
	});
}

#[test]
fn grow_empty_embed() {
	let fs = new();
	run(&fs, async {
		let file = mkfile(&fs.root_dir(), b"file").await;
		file.resize(1).await.unwrap().unwrap();
		file.resize(0).await.unwrap().unwrap();
	});
}

#[test]
fn write_edge() {
	let fs = new();
	run(&fs, async {
		let file = mkfile(&fs.root_dir(), b"file").await;
		file.resize(20).await.unwrap().unwrap();
		let len = file.write(15, &[1; 30]).await.unwrap();
		assert_eq!(len, 20 - 15);
	});
}

#[test]
fn read_embed() {
	let fs = new();
	run(&fs, async {
		let file = mkfile(&fs.root_dir(), b"file").await;
		file.write_grow(2, &[1, 2]).await.unwrap().unwrap();
		let mut buf = [0xff; 4];
		let len = file.read(1, &mut buf).await.unwrap();
		assert_eq!(len, 3);
		assert_eq!(buf, [0, 1, 2, 0xff]);
	});
}

#[test]
fn write_embed() {
	let fs = new();
	run(&fs, async {
		let f = mkfile(&fs.root_dir(), b"x").await;
		f.write_grow(1, &[1; 230]).await.unwrap().unwrap();
		f.write(0, &[1; 15359]).await.unwrap();
		let mut buf = [2; 231];
		let l = f.read(0, &mut buf).await.unwrap();
		assert_eq!(l, 231);
		assert_eq!(buf, [1; 231]);
	})
}
