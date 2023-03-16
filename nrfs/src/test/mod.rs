mod dir;
mod file;
pub mod fuzz;

use {
	crate::*,
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

fn run<'a, F>(fs: &Nrfs<MemDev>, f: F)
where
	F: Future<Output = ()>,
{
	block_on(fs.run(async { Ok::<_, Error<_>>(f.await) })).unwrap()
}

fn new() -> Nrfs<MemDev> {
	new_cap(1 << 10, BlockSize::K1, MaxRecordSize::K1)
}

fn new_cap(size: usize, block_size: BlockSize, max_record_size: MaxRecordSize) -> Nrfs<MemDev> {
	block_on(Nrfs::new(NewConfig {
		key_deriver: KeyDeriver::None { key: &[0; 32] },
		cipher: CipherType::NoneXxh3,
		mirrors: vec![vec![MemDev::new(size, block_size)]],
		block_size,
		max_record_size,
		compression: Compression::None,
		cache_size: 4096,
		dir: Default::default(),
	}))
	.unwrap()
}

/// New filesystem with extensions.
fn new_ext() -> Nrfs<MemDev> {
	block_on(Nrfs::new(NewConfig {
		key_deriver: KeyDeriver::None { key: &[0; 32] },
		cipher: CipherType::NoneXxh3,
		mirrors: vec![vec![MemDev::new(1 << 10, BlockSize::K1)]],
		block_size: BlockSize::K1,
		max_record_size: MaxRecordSize::K1,
		dir: *EnableExt::default().add_unix().add_mtime(),
		compression: Compression::None,
		cache_size: 4096,
	}))
	.unwrap()
}

async fn mkdir<'a>(dir: &Dir<'a, MemDev>, name: &[u8], ext: ItemExt) -> Dir<'a, MemDev> {
	dir.create_dir(name.try_into().unwrap(), Default::default(), ext)
		.await
		.unwrap()
		.unwrap()
}

async fn mkfile<'a>(dir: &Dir<'a, MemDev>, name: &[u8], ext: ItemExt) -> File<'a, MemDev> {
	dir.create_file(name.try_into().unwrap(), ext)
		.await
		.unwrap()
		.unwrap()
}

#[test]
fn create_fs() {
	new();
}

#[test]
fn create_file() {
	let fs = new();
	run(&fs, async {
		let d = fs.root_dir();
		let f = d
			.create_file(b"test.txt".into(), Default::default())
			.await
			.unwrap()
			.unwrap();
		f.write_grow(0, b"Hello, world!").await.unwrap().unwrap();

		assert!(d.search(b"I do not exist".into()).await.unwrap().is_none());

		let data = d.search(b"test.txt".into()).await.unwrap().unwrap();
		assert!(data.ext.unix.is_none());
		assert!(data.ext.mtime.is_none());
		let f = data.key().into_file().unwrap();

		let mut buf = [0; 32];
		let l = File::new(&fs, f).read(0, &mut buf).await.unwrap();
		assert_eq!(l, b"Hello, world!".len());
		assert_eq!(core::str::from_utf8(&buf[..l]), Ok("Hello, world!"));
	});
}

#[test]
fn create_many_files() {
	let fs = new();
	run(&fs, async {
		// Create & read
		for i in 0..100 {
			let name = format!("{}.txt", i);
			let contents = format!("This is file #{}", i);

			let d = fs.root_dir();
			let f = d
				.create_file(<&Name>::try_from(&*name).unwrap(), Default::default())
				.await
				.unwrap()
				.unwrap();
			f.write_grow(0, contents.as_bytes()).await.unwrap().unwrap();

			let file = d
				.search((&*name).try_into().unwrap())
				.await
				.unwrap()
				.unwrap();
			let file = file.key().into_file().unwrap();

			let mut buf = [0; 32];
			let l = File::new(&fs, file).read(0, &mut buf).await.unwrap();
			assert_eq!(core::str::from_utf8(&buf[..l]), Ok(&*contents),);

			fs.finish_transaction().await.unwrap();
		}

		// Test iteration
		let d = fs.root_dir();
		let mut i = 0;
		let mut count = 0;
		while let Some((_, ni)) = d.next_from(i).await.unwrap() {
			count += 1;
			i = ni;
		}
		assert_eq!(count, 100);

		// Read only
		for i in 0..100 {
			let name = format!("{}.txt", i);
			let contents = format!("This is file #{}", i);

			let file = fs
				.root_dir()
				.search((&*name).try_into().unwrap())
				.await
				.unwrap()
				.unwrap();
			let file = file.key().into_file().unwrap();

			let mut buf = [0; 32];
			let l = File::new(&fs, file).read(0, &mut buf).await.unwrap();
			assert_eq!(
				core::str::from_utf8(&buf[..l]),
				Ok(&*contents),
				"file #{}",
				i
			);
		}
	});
}

#[test]
fn create_file_ext() {
	let fs = new_ext();

	run(&fs, async {
		let d = fs.root_dir();
		let ext = ItemExt {
			unix: Some(ext::Unix::new(0o640, 1000, 1001)),
			mtime: Some(ext::MTime { mtime: 0xdead }),
		};
		let f = d
			.create_file(b"test.txt".into(), ext)
			.await
			.unwrap()
			.unwrap();
		f.write_grow(0, b"Hello, world!").await.unwrap().unwrap();

		assert!(d.search(b"I do not exist".into()).await.unwrap().is_none());

		let data = d.search(b"test.txt".into()).await.unwrap().unwrap();
		assert_eq!(data.ext.unix.unwrap().permissions, 0o640);
		assert_eq!(data.ext.unix.unwrap().uid(), 1000);
		assert_eq!(data.ext.unix.unwrap().gid(), 1001);
		assert_eq!(data.ext.mtime.unwrap().mtime, 0xdead);
		let f = data.key().into_file().unwrap();

		let mut buf = [0; 32];
		let l = File::new(&fs, f).read(0, &mut buf).await.unwrap();
		assert_eq!(core::str::from_utf8(&buf[..l]), Ok("Hello, world!"));
	});
}

#[test]
fn destroy_file() {
	let fs = new();
	run(&fs, async {
		let d = fs.root_dir();

		d.create_file(b"hello".into(), Default::default())
			.await
			.unwrap()
			.unwrap();
		d.create_file(b"world".into(), Default::default())
			.await
			.unwrap()
			.unwrap();
		d.create_file(b"exist".into(), Default::default())
			.await
			.unwrap()
			.unwrap();

		let file = d.search(b"hello".into()).await.unwrap().unwrap();
		let file = file.key().into_file().unwrap();
		File::new(&fs, file).destroy().await.unwrap();

		// Ensure no spooky entries appear when iterating
		let mut i = 0;
		while let Some((e, ni)) = d.next_from(i).await.unwrap() {
			assert!(matches!(&**e.name, b"world" | b"exist"));
			i = ni;
		}
	});
}

#[test]
fn remount() {
	let fs = new();
	let devices = block_on(fs.unmount()).unwrap();
	block_on(Nrfs::load(LoadConfig {
		devices,
		cache_size: 1 << 12,
		allow_repair: true,
		retrieve_key: &mut |_| unreachable!(),
	}))
	.unwrap();
}

#[test]
fn remount_ext_file() {
	let fs = new_ext();
	run(&fs, async {
		mkfile(
			&fs.root_dir(),
			b"file",
			ItemExt { unix: Some(Unix::new(0x7ead, 42, 1337)), ..Default::default() },
		)
		.await;
	});
	let devices = block_on(fs.unmount()).unwrap();
	let fs = block_on(Nrfs::load(LoadConfig {
		devices,
		cache_size: 1 << 12,
		allow_repair: true,
		retrieve_key: &mut |_| unreachable!(),
	}))
	.unwrap();
	run(&fs, async {
		let file = fs.root_dir().search(b"file".into()).await.unwrap().unwrap();
		assert_eq!(file.ext.unix.unwrap().permissions, 0x7ead);
		assert_eq!(file.ext.unix.unwrap().uid(), 42);
		assert_eq!(file.ext.unix.unwrap().gid(), 1337);
	});
}

#[test]
fn remount_ext_root() {
	let fs = new_ext();
	run(&fs, async {
		let root = fs.root_dir().key;
		let root = fs.item(ItemKey::Dir(root));
		root.set_unix(Unix::new(0x7ead, 42, 1337)).await.unwrap();
	});
	let devices = block_on(fs.unmount()).unwrap();
	let fs = block_on(Nrfs::load(LoadConfig {
		devices,
		cache_size: 1 << 12,
		allow_repair: true,
		retrieve_key: &mut |_| unreachable!(),
	}))
	.unwrap();
	run(&fs, async {
		let root = fs.root_dir().key;
		let root = fs.item(ItemKey::Dir(root));
		let ext = root.ext().await.unwrap();
		assert_eq!(ext.unix.unwrap().permissions, 0x7ead);
		assert_eq!(ext.unix.unwrap().uid(), 42);
		assert_eq!(ext.unix.unwrap().gid(), 1337);
	});
}
