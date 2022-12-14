use {
	crate::*,
	core::{
		future::Future,
		pin::pin,
		task::{Context, Poll},
	},
	nros::dev::MemDev,
};

fn run<F, R>(fut: F) -> R
where
	F: Future<Output = R>,
{
	let mut fut = pin!(fut);
	let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
	loop {
		if let Poll::Ready(r) = fut.as_mut().poll(&mut cx) {
			return r;
		}
	}
}

async fn new() -> Nrfs<MemDev> {
	Nrfs::new(
		[[MemDev::new(1 << 10, BlockSize::K1)]],
		BlockSize::K1,
		MaxRecordSize::K1,
		&Default::default(),
		Compression::None,
		1 << 12,
		1 << 12,
	)
	.await
	.unwrap()
}

/// New filesystem with extensions.
async fn new_ext() -> Nrfs<MemDev> {
	Nrfs::new(
		[[MemDev::new(1 << 10, BlockSize::K1)]],
		BlockSize::K1,
		MaxRecordSize::K1,
		&DirOptions {
			extensions: *dir::EnableExtensions::default().add_unix().add_mtime(),
			..Default::default()
		},
		Compression::None,
		4096,
		4096,
	)
	.await
	.unwrap()
}

#[test]
fn create_fs() {
	run(new());
}

#[test]
fn create_file() {
	run(async {
		let mut fs = new().await;

		let mut d = fs.root_dir().await.unwrap();
		let mut f = d
			.create_file(b"test.txt".into(), &Default::default())
			.await
			.unwrap()
			.unwrap();
		f.write_grow(0, b"Hello, world!").await.unwrap();

		assert!(d.find(b"I do not exist".into()).await.unwrap().is_none());
		let mut g = d.find(b"test.txt".into()).await.unwrap().unwrap();
		assert!(g.ext_unix().is_none());
		assert!(g.ext_mtime().is_none());

		let mut buf = [0; 32];
		let l = g.as_file().unwrap().read(0, &mut buf).await.unwrap();
		assert_eq!(core::str::from_utf8(&buf[..l]), Ok("Hello, world!"));
	})
}

#[test]
fn create_many_files() {
	run(async {
		let mut fs = new().await;

		// Create & read
		for i in 0..100 {
			let name = format!("{}.txt", i);
			let contents = format!("This is file #{}", i);

			let mut d = fs.root_dir().await.unwrap();
			let mut f = d
				.create_file((&*name).try_into().unwrap(), &Default::default())
				.await
				.unwrap()
				.unwrap();
			f.write_grow(0, contents.as_bytes()).await.unwrap();

			let mut g = d.find((&*name).try_into().unwrap()).await.unwrap().unwrap();

			let mut buf = [0; 32];
			let l = g.as_file().unwrap().read(0, &mut buf).await.unwrap();
			assert_eq!(
				core::str::from_utf8(&buf[..l]),
				Ok(&*contents),
				"file #{}",
				i
			);

			fs.finish_transaction().await.unwrap();
		}

		// Test iteration
		let mut d = fs.root_dir().await.unwrap();
		let mut i = 0;
		let mut count = 0;
		while let Some((_, ni)) = d.next_from(i).await.unwrap() {
			count += 1;
			i = if let Some(i) = ni { i } else { break };
		}
		assert_eq!(count, 100);

		// Read only
		for i in 0..100 {
			let name = format!("{}.txt", i);
			let contents = format!("This is file #{}", i);

			let mut d = fs.root_dir().await.unwrap();

			let mut g = d.find((&*name).try_into().unwrap()).await.unwrap().unwrap();

			let mut buf = [0; 32];
			let l = g.as_file().unwrap().read(0, &mut buf).await.unwrap();
			assert_eq!(
				core::str::from_utf8(&buf[..l]),
				Ok(&*contents),
				"file #{}",
				i
			);
		}
	})
}

#[test]
fn create_file_ext() {
	run(async {
		let mut fs = new_ext().await;

		let mut d = fs.root_dir().await.unwrap();
		let mut f = d
			.create_file(
				b"test.txt".into(),
				&dir::Extensions {
					unix: Some(dir::ext::unix::Entry { permissions: 0o640, uid: 1000, gid: 1001 }),
					mtime: Some(dir::ext::mtime::Entry { mtime: 0xdead }),
				},
			)
			.await
			.unwrap()
			.unwrap();
		f.write_grow(0, b"Hello, world!").await.unwrap();

		assert!(d.find(b"I do not exist".into()).await.unwrap().is_none());
		let mut g = d.find(b"test.txt".into()).await.unwrap().unwrap();
		assert_eq!(g.ext_unix().unwrap().permissions, 0o640);
		assert_eq!(g.ext_unix().unwrap().uid, 1000);
		assert_eq!(g.ext_unix().unwrap().gid, 1001);
		assert_eq!(g.ext_mtime().unwrap().mtime, 0xdead);

		let mut buf = [0; 32];
		let l = g.as_file().unwrap().read(0, &mut buf).await.unwrap();
		assert_eq!(core::str::from_utf8(&buf[..l]), Ok("Hello, world!"));
	})
}

#[test]
fn remove_file() {
	run(async {
		let mut fs = new().await;
		let mut d = fs.root_dir().await.unwrap();
		assert_eq!(d.len(), 0);
		d.create_file(b"hello".into(), &Default::default())
			.await
			.unwrap();
		assert_eq!(d.len(), 1);
		d.create_file(b"world".into(), &Default::default())
			.await
			.unwrap();
		assert_eq!(d.len(), 2);
		d.create_file(b"exist".into(), &Default::default())
			.await
			.unwrap();
		assert_eq!(d.len(), 3);
		assert!(d.remove(b"hello".into()).await.unwrap());
		assert_eq!(d.len(), 2);

		// Ensure no spooky entries appear when iterating
		let mut i = Some(0);
		while let Some((e, ni)) = async {
			let i = i?;
			d.next_from(i).await.unwrap()
		}
		.await
		{
			assert!(matches!(&**e.name(), b"world" | b"exist"));
			i = ni;
		}
	})
}

#[test]
fn shrink() {
	run(async {
		let mut fs = new().await;
		let mut d = fs.root_dir().await.unwrap();
		assert_eq!(d.len(), 0);
		d.create_file(b"hello".into(), &Default::default())
			.await
			.unwrap();
		assert_eq!(d.len(), 1);
		d.create_file(b"world".into(), &Default::default())
			.await
			.unwrap();
		assert_eq!(d.len(), 2);
		d.create_file(b"exist".into(), &Default::default())
			.await
			.unwrap();
		assert_eq!(d.len(), 3);
		assert!(d.remove(b"hello".into()).await.unwrap());
		assert_eq!(d.len(), 2);
		assert!(d.remove(b"exist".into()).await.unwrap());
		assert_eq!(d.len(), 1);

		// Ensure no spooky entries appear when iterating
		let mut i = Some(0);
		while let Some((e, ni)) = async {
			let i = i?;
			d.next_from(i).await.unwrap()
		}
		.await
		{
			assert!(matches!(&**e.name(), b"world"));
			i = ni;
		}
	})
}

#[test]
fn remove_collision() {
	run(async {
		let mut fs = new().await;
		let mut d = fs.root_dir().await.unwrap();
		// NOTE: hash must be SipHash13 and key must be 0
		// Insert files to avoid shrinking below 8
		for i in 0..5 {
			d.create_file((&[i]).into(), &Default::default())
				.await
				.unwrap();
		}
		d.create_file(b"d".into(), &Default::default())
			.await
			.unwrap(); // c4eafac0
		d.create_file(b"g".into(), &Default::default())
			.await
			.unwrap(); // e57630a8
		assert!(d.remove(b"d".into()).await.unwrap());
		// If the hashmap is improperly implemented, the empty slot makes
		// it impossible to find "g" with linear probing
		assert!(d.remove(b"g".into()).await.unwrap());
	})
}

#[test]
fn real_case_find_000_minified() {
	run(async {
		let mut fs = new().await;
		let mut d = fs.root_dir().await.unwrap();
		d.create_dir(b"d".into(), &Default::default(), &Default::default())
			.await
			.unwrap();
		d.create_file(b"C".into(), &Default::default())
			.await
			.unwrap();
		d.create_file(b".rustc_info.json".into(), &Default::default())
			.await
			.unwrap();
		d.create_dir(b"p".into(), &Default::default(), &Default::default())
			.await
			.unwrap();
		assert_eq!(d.len(), fs.root_dir().await.unwrap().len());
		let mut d = fs.root_dir().await.unwrap();
		d.find(b".rustc_info.json".into()).await.unwrap().unwrap();
	})
}
