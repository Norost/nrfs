use crate::*;

type S = nros::storage::MemoryDev;

fn new() -> Nrfs<S> {
	let s = S::new(1 << 10, 10);
	Nrfs::new(
		s,
		MaxRecordSize::K1,
		&Default::default(),
		Compression::None,
		32,
	)
	.unwrap()
}

/// New filesystem with extensions.
fn new_ext() -> Nrfs<S> {
	Nrfs::new(
		S::new(1 << 10, 10),
		MaxRecordSize::K1,
		&DirOptions {
			extensions: *dir::EnableExtensions::default().add_unix().add_mtime(),
			..Default::default()
		},
		Compression::None,
		32,
	)
	.unwrap()
}

#[test]
fn create_fs() {
	new();
}

#[test]
fn create_file() {
	let mut fs = new();

	let mut d = fs.root_dir().unwrap();
	let mut f = d
		.create_file(b"test.txt".into(), &Default::default())
		.unwrap()
		.unwrap();
	f.write_grow(0, b"Hello, world!").unwrap();

	assert!(d.find(b"I do not exist".into()).unwrap().is_none());
	let mut g = d.find(b"test.txt".into()).unwrap().unwrap();
	assert!(g.ext_unix().is_none());
	assert!(g.ext_mtime().is_none());

	let mut buf = [0; 32];
	let l = g.as_file().unwrap().read(0, &mut buf).unwrap();
	assert_eq!(core::str::from_utf8(&buf[..l]), Ok("Hello, world!"));
}

#[test]
fn create_many_files() {
	let mut fs = new();

	// Create & read
	for i in 0..100 {
		let name = format!("{}.txt", i);
		let contents = format!("This is file #{}", i);

		let mut d = fs.root_dir().unwrap();
		let mut f = d
			.create_file((&*name).try_into().unwrap(), &Default::default())
			.unwrap()
			.unwrap();
		f.write_grow(0, contents.as_bytes()).unwrap();

		let mut g = d.find((&*name).try_into().unwrap()).unwrap().unwrap();

		let mut buf = [0; 32];
		let l = g.as_file().unwrap().read(0, &mut buf).unwrap();
		assert_eq!(
			core::str::from_utf8(&buf[..l]),
			Ok(&*contents),
			"file #{}",
			i
		);

		fs.finish_transaction().unwrap();
	}

	// Test iteration
	let mut d = fs.root_dir().unwrap();
	let mut i = 0;
	let mut count = 0;
	while let Some((_, ni)) = d.next_from(i).unwrap() {
		count += 1;
		i = if let Some(i) = ni { i } else { break };
	}
	assert_eq!(count, 100);

	// Read only
	for i in 0..100 {
		let name = format!("{}.txt", i);
		let contents = format!("This is file #{}", i);

		let mut d = fs.root_dir().unwrap();

		let mut g = d.find((&*name).try_into().unwrap()).unwrap().unwrap();

		let mut buf = [0; 32];
		let l = g.as_file().unwrap().read(0, &mut buf).unwrap();
		assert_eq!(
			core::str::from_utf8(&buf[..l]),
			Ok(&*contents),
			"file #{}",
			i
		);
	}
}

#[test]
fn create_file_ext() {
	let mut fs = new_ext();

	let mut d = fs.root_dir().unwrap();
	let mut f = d
		.create_file(
			b"test.txt".into(),
			&dir::Extensions {
				unix: Some(dir::ext::unix::Entry { permissions: 0o640, uid: 1000, gid: 1001 }),
				mtime: Some(dir::ext::mtime::Entry { mtime: 0xdead }),
			},
		)
		.unwrap()
		.unwrap();
	f.write_grow(0, b"Hello, world!").unwrap();

	assert!(d.find(b"I do not exist".into()).unwrap().is_none());
	let mut g = d.find(b"test.txt".into()).unwrap().unwrap();
	assert_eq!(g.ext_unix().unwrap().permissions, 0o640);
	assert_eq!(g.ext_unix().unwrap().uid, 1000);
	assert_eq!(g.ext_unix().unwrap().gid, 1001);
	assert_eq!(g.ext_mtime().unwrap().mtime, 0xdead);

	let mut buf = [0; 32];
	let l = g.as_file().unwrap().read(0, &mut buf).unwrap();
	assert_eq!(core::str::from_utf8(&buf[..l]), Ok("Hello, world!"));
}

#[test]
fn remove_file() {
	let mut fs = new();
	let mut d = fs.root_dir().unwrap();
	assert_eq!(d.len(), 0);
	d.create_file(b"hello".into(), &Default::default()).unwrap();
	assert_eq!(d.len(), 1);
	d.create_file(b"world".into(), &Default::default()).unwrap();
	assert_eq!(d.len(), 2);
	d.create_file(b"exist".into(), &Default::default()).unwrap();
	assert_eq!(d.len(), 3);
	assert!(d.remove(b"hello".into()).unwrap());
	assert_eq!(d.len(), 2);

	// Ensure no spooky entries appear when iterating
	let mut i = Some(0);
	while let Some((e, ni)) = i.and_then(|i| d.next_from(i).unwrap()) {
		assert!(matches!(&**e.name(), b"world" | b"exist"));
		i = ni;
	}
}

#[test]
fn shrink() {
	let mut fs = new();
	let mut d = fs.root_dir().unwrap();
	assert_eq!(d.len(), 0);
	d.create_file(b"hello".into(), &Default::default()).unwrap();
	assert_eq!(d.len(), 1);
	d.create_file(b"world".into(), &Default::default()).unwrap();
	assert_eq!(d.len(), 2);
	d.create_file(b"exist".into(), &Default::default()).unwrap();
	assert_eq!(d.len(), 3);
	assert!(d.remove(b"hello".into()).unwrap());
	assert_eq!(d.len(), 2);
	assert!(d.remove(b"exist".into()).unwrap());
	assert_eq!(d.len(), 1);

	// Ensure no spooky entries appear when iterating
	let mut i = Some(0);
	while let Some((e, ni)) = i.and_then(|i| d.next_from(i).unwrap()) {
		assert!(matches!(&**e.name(), b"world"));
		i = ni;
	}
}

#[test]
fn remove_collision() {
	let mut fs = new();
	let mut d = fs.root_dir().unwrap();
	// NOTE: hash must be SipHash13 and key must be 0
	// Insert files to avoid shrinking below 8
	for i in 0..5 {
		d.create_file((&[i]).into(), &Default::default()).unwrap();
	}
	d.create_file(b"d".into(), &Default::default()).unwrap(); // c4eafac0
	d.create_file(b"g".into(), &Default::default()).unwrap(); // e57630a8
	assert!(d.remove(b"d".into()).unwrap());
	// If the hashmap is improperly implemented, the empty slot makes
	// it impossible to find "g" with linear probing
	assert!(d.remove(b"g".into()).unwrap());
}

#[test]
fn real_case_find_000_minified() {
	let mut fs = new();
	let mut d = fs.root_dir().unwrap();
	d.create_dir(b"d".into(), &Default::default(), &Default::default())
		.unwrap();
	d.create_file(b"C".into(), &Default::default()).unwrap();
	d.create_file(b".rustc_info.json".into(), &Default::default())
		.unwrap();
	d.create_dir(b"p".into(), &Default::default(), &Default::default())
		.unwrap();
	assert_eq!(d.len(), fs.root_dir().unwrap().len());
	let mut d = fs.root_dir().unwrap();
	d.find(b".rustc_info.json".into()).unwrap().unwrap();
}
