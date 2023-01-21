mod dir;
mod file;
pub mod fuzz;

use {
	crate::{
		dir::{ext, EnableExtensions, Extensions},
		Background, *,
	},
	core::{
		future::Future,
		task::{Context, Poll},
	},
	nros::dev::MemDev,
};

fn block_on<R>(fut: impl Future<Output = R>) -> R {
	let mut fut = core::pin::pin!(fut);
	let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
	loop {
		if let Poll::Ready(r) = fut.as_mut().poll(&mut cx) {
			return r;
		}
	}
}

fn run2<'a, 'b, F>(bg: &'b Background<'a, MemDev>, f: F)
where
	F: Future<Output = ()>,
{
	block_on(bg.run(async { Ok(f.await) })).unwrap()
}

fn new() -> Nrfs<MemDev> {
	new_cap(1 << 10, BlockSize::K1, MaxRecordSize::K1)
}

fn new_cap(size: usize, block_size: BlockSize, max_record_size: MaxRecordSize) -> Nrfs<MemDev> {
	block_on(Nrfs::new(
		[[MemDev::new(size, block_size)]],
		block_size,
		max_record_size,
		&DirOptions::new(&[0; 16]),
		Compression::None,
		1 << 12,
	))
	.unwrap()
}

/// New filesystem with extensions.
fn new_ext() -> Nrfs<MemDev> {
	block_on(Nrfs::new(
		[[MemDev::new(1 << 10, BlockSize::K1)]],
		BlockSize::K1,
		MaxRecordSize::K1,
		&DirOptions {
			extensions: *EnableExtensions::default().add_unix().add_mtime(),
			..DirOptions::new(&[0; 16])
		},
		Compression::None,
		4096,
	))
	.unwrap()
}

#[test]
fn create_fs() {
	new();
}

/// BorrowMutErrors in Drop impls are annoying as hell
#[test]
fn drop_borrow() {
	let fs = new();
	let bg = Background::default();
	run2(&bg, async {
		let dir = fs.root_dir(&bg).await.unwrap();
		dir.create_file(b"test.txt".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		dir.drop().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn create_file() {
	let fs = new();
	let bg = Background::default();
	run2(&bg, async {
		let d = fs.root_dir(&bg).await.unwrap();
		let f = d
			.create_file(b"test.txt".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();
		f.write_grow(0, b"Hello, world!").await.unwrap();

		assert!(d.find(b"I do not exist".into()).await.unwrap().is_none());

		let file = d.find(b"test.txt".into()).await.unwrap().unwrap();
		let data = file.data().await.unwrap();
		assert!(data.ext_unix.is_none());
		assert!(data.ext_mtime.is_none());

		let mut buf = [0; 32];
		let ItemRef::File(file) = file else { panic!("expected file") };
		let l = file.read(0, &mut buf).await.unwrap();
		assert_eq!(core::str::from_utf8(&buf[..l]), Ok("Hello, world!"));

		f.drop().await.unwrap();
		file.drop().await.unwrap();
		d.drop().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn create_many_files() {
	let fs = new();
	let bg = Background::default();
	run2(&bg, async {
		// Create & read
		for i in 0..100 {
			let name = format!("{}.txt", i);
			let contents = format!("This is file #{}", i);

			let d = fs.root_dir(&bg).await.unwrap();
			let f = d
				.create_file((&*name).try_into().unwrap(), &Default::default())
				.await
				.unwrap()
				.unwrap();
			f.write_grow(0, contents.as_bytes()).await.unwrap();

			let file = d.find((&*name).try_into().unwrap()).await.unwrap().unwrap();

			let mut buf = [0; 32];
			let ItemRef::File(file) = file else { panic!("expected file") };
			let l = file.read(0, &mut buf).await.unwrap();
			assert_eq!(core::str::from_utf8(&buf[..l]), Ok(&*contents),);

			fs.finish_transaction(&bg).await.unwrap();

			file.drop().await.unwrap();
			f.drop().await.unwrap();
			d.drop().await.unwrap();
		}

		// Test iteration
		let d = fs.root_dir(&bg).await.unwrap();
		let mut i = 0;
		let mut count = 0;
		while let Some((e, ni)) = d.next_from(i).await.unwrap() {
			count += 1;
			i = ni;
			e.drop().await.unwrap();
		}
		assert_eq!(count, 100);

		// Read only
		for i in 0..100 {
			let name = format!("{}.txt", i);
			let contents = format!("This is file #{}", i);

			let d = fs.root_dir(&bg).await.unwrap();

			let file = d.find((&*name).try_into().unwrap()).await.unwrap().unwrap();

			let mut buf = [0; 32];
			let ItemRef::File(file) = file else { panic!("expected file") };
			let l = file.read(0, &mut buf).await.unwrap();
			assert_eq!(
				core::str::from_utf8(&buf[..l]),
				Ok(&*contents),
				"file #{}",
				i
			);

			file.drop().await.unwrap();
			d.drop().await.unwrap();
		}

		d.drop().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn create_file_ext() {
	let fs = new_ext();
	let bg = Background::default();
	run2(&bg, async {
		let d = fs.root_dir(&bg).await.unwrap();
		let f = d
			.create_file(
				b"test.txt".into(),
				&Extensions {
					unix: Some(ext::unix::Entry::new(0o640, 1000, 1001)),
					mtime: Some(ext::mtime::Entry { mtime: 0xdead }),
				},
			)
			.await
			.unwrap()
			.unwrap();
		f.write_grow(0, b"Hello, world!").await.unwrap();

		assert!(d.find(b"I do not exist".into()).await.unwrap().is_none());

		let file = d.find(b"test.txt".into()).await.unwrap().unwrap();
		let data = file.data().await.unwrap();
		assert_eq!(data.ext_unix.unwrap().permissions, 0o640);
		assert_eq!(data.ext_unix.unwrap().uid(), 1000);
		assert_eq!(data.ext_unix.unwrap().gid(), 1001);
		assert_eq!(data.ext_mtime.unwrap().mtime, 0xdead);

		let mut buf = [0; 32];
		let ItemRef::File(file) = file else { panic!("expected file") };
		let l = file.read(0, &mut buf).await.unwrap();
		assert_eq!(core::str::from_utf8(&buf[..l]), Ok("Hello, world!"));

		file.drop().await.unwrap();
		f.drop().await.unwrap();
		d.drop().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn remove_file() {
	let fs = new();
	let bg = Background::default();
	run2(&bg, async {
		let d = fs.root_dir(&bg).await.unwrap();
		assert_eq!(d.len().await.unwrap(), 0);

		d.create_file(b"hello".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		assert_eq!(d.len().await.unwrap(), 1);

		d.create_file(b"world".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		assert_eq!(d.len().await.unwrap(), 2);

		d.create_file(b"exist".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		assert_eq!(d.len().await.unwrap(), 3);

		d.remove(b"hello".into()).await.unwrap().unwrap();
		assert_eq!(d.len().await.unwrap(), 2);

		// Ensure no spooky entries appear when iterating
		let mut i = 0;
		while let Some((e, ni)) = d.next_from(i).await.unwrap() {
			let data = e.data().await.unwrap();
			let key = e.key(&data).await.unwrap();
			assert!(matches!(&**key.unwrap(), b"world" | b"exist"));
			i = ni;
			e.drop().await.unwrap();
		}

		d.drop().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn shrink() {
	let fs = new();
	let bg = Background::default();
	run2(&bg, async {
		let d = fs.root_dir(&bg).await.unwrap();
		assert_eq!(d.len().await.unwrap(), 0);
		d.create_file(b"hello".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		assert_eq!(d.len().await.unwrap(), 1);
		d.create_file(b"world".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		assert_eq!(d.len().await.unwrap(), 2);
		d.create_file(b"exist".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		assert_eq!(d.len().await.unwrap(), 3);
		d.remove(b"hello".into()).await.unwrap().unwrap();
		assert_eq!(d.len().await.unwrap(), 2);
		d.remove(b"exist".into()).await.unwrap().unwrap();
		assert_eq!(d.len().await.unwrap(), 1);

		// Ensure no spooky entries appear when iterating
		let mut i = 0;
		while let Some((e, ni)) = d.next_from(i).await.unwrap() {
			let data = e.data().await.unwrap();
			let key = e.key(&data).await.unwrap();
			assert_eq!(&**key.unwrap(), b"world");
			i = ni;
			e.drop().await.unwrap();
		}

		d.drop().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

/// Attempt to find all files even with collisions.
#[test]
fn find_colllision() {
	let fs = new();
	let bg = Background::default();
	run2(&bg, async {
		let d = fs.root_dir(&bg).await.unwrap();
		// NOTE: hash must be SipHash13 and key must be 0
		// Insert files to avoid shrinking below 8
		for i in 0..5 {
			d.create_file((&[i]).into(), &Default::default())
				.await
				.unwrap()
				.unwrap()
				.drop()
				.await
				.unwrap();
		}
		d.create_file(b"d".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap(); // c4eafac0
		d.create_file(b"g".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap(); // e57630a8

		d.find(b"\x00".into())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		d.find(b"\x01".into())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		d.find(b"\x02".into())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		d.find(b"\x03".into())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		d.find(b"\x04".into())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		d.find(b"d".into())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		d.find(b"g".into())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();

		d.drop().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn remove_collision() {
	let fs = new();
	let bg = Background::default();
	run2(&bg, async {
		let d = fs.root_dir(&bg).await.unwrap();
		// NOTE: hash must be SipHash13 and key must be 0
		// Insert files to avoid shrinking below 8
		for i in 0..5 {
			d.create_file((&[i]).into(), &Default::default())
				.await
				.unwrap()
				.unwrap()
				.drop()
				.await
				.unwrap();
		}
		d.create_file(b"d".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap(); // c4eafac0
		d.create_file(b"g".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap(); // e57630a8
		d.remove(b"d".into()).await.unwrap().unwrap();
		// If the hashmap is improperly implemented, the empty slot makes
		// it impossible to find "g" with linear probing
		d.remove(b"g".into()).await.unwrap().unwrap();

		d.drop().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn real_case_find_000_minified() {
	let fs = new();
	let bg = Background::default();
	run2(&bg, async {
		let d = fs.root_dir(&bg).await.unwrap();
		d.create_dir(b"d".into(), &DirOptions::new(&[0; 16]), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		d.create_file(b"C".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		d.create_file(b".rustc_info.json".into(), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		d.create_dir(b"p".into(), &DirOptions::new(&[0; 16]), &Default::default())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();
		let e = fs.root_dir(&bg).await.unwrap();
		assert_eq!(d.len().await.unwrap(), e.len().await.unwrap());
		e.find(b".rustc_info.json".into())
			.await
			.unwrap()
			.unwrap()
			.drop()
			.await
			.unwrap();

		e.drop().await.unwrap();
		d.drop().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}
