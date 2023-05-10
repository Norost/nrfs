mod attr;
mod dir;
mod file;

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
	new_cap(1 << 10, BlockSize::K1, MaxRecordSize::K1, 4096)
}

fn new_cap(
	size: usize,
	block_size: BlockSize,
	max_record_size: MaxRecordSize,
	cache_size: usize,
) -> Nrfs<MemDev> {
	block_on(Nrfs::new(NewConfig {
		key_deriver: KeyDeriver::None { key: &[0; 32] },
		cipher: CipherType::NoneXxh3,
		mirrors: vec![vec![MemDev::new(size, block_size)]],
		block_size,
		max_record_size,
		compression: Compression::None,
		cache_size,
	}))
	.unwrap()
}

async fn mkdir<'a>(dir: &Dir<'a, MemDev>, name: &[u8]) -> Dir<'a, MemDev> {
	dir.create_dir(name.try_into().unwrap())
		.await
		.unwrap()
		.unwrap()
}

async fn mkfile<'a>(dir: &Dir<'a, MemDev>, name: &[u8]) -> File<'a, MemDev> {
	dir.create_file(name.try_into().unwrap())
		.await
		.unwrap()
		.unwrap()
}

async fn mksym<'a>(dir: &Dir<'a, MemDev>, name: &[u8]) -> File<'a, MemDev> {
	dir.create_sym(name.try_into().unwrap())
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
		let f = d.create_file(b"test.txt".into()).await.unwrap().unwrap();
		f.write_grow(0, b"Hello, world!").await.unwrap().unwrap();

		assert!(d.search(b"I do not exist".into()).await.unwrap().is_none());

		let f = d.search(b"test.txt".into()).await.unwrap().unwrap();

		let mut buf = [0; 32];
		let l = fs.file(f.key).read(0, &mut buf).await.unwrap();
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
				.create_file(<&Key>::try_from(&*name).unwrap())
				.await
				.unwrap()
				.unwrap();
			f.write_grow(0, contents.as_bytes()).await.unwrap().unwrap();

			let file = d
				.search((&*name).try_into().unwrap())
				.await
				.unwrap()
				.unwrap();

			let mut buf = [0; 32];
			let l = fs.file(file.key).read(0, &mut buf).await.unwrap();
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

			let mut buf = [0; 32];
			let l = fs.file(file.key).read(0, &mut buf).await.unwrap();
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
fn destroy_file() {
	let fs = new();
	run(&fs, async {
		let d = fs.root_dir();

		let f = d.create_file(b"hello".into()).await.unwrap().unwrap();
		d.create_file(b"world".into()).await.unwrap().unwrap();
		d.create_file(b"exist".into()).await.unwrap().unwrap();

		d.remove(f.key()).await.unwrap().unwrap();

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
