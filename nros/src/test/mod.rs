mod allocator;
mod cache;
mod dev;
mod record;

use {
	crate::*,
	core::{future::Future, task::Context},
};

async fn new(max_record_size: MaxRecordSize) -> Nros<MemDev> {
	let s = MemDev::new(16, BlockSize::K1);
	Nros::new(
		[[s]],
		BlockSize::K1,
		max_record_size,
		Compression::None,
		// Don't evict cache for tests with small amounts of data, effectively.
		4 * 1024,
		4 * 1024,
	)
	.await
	.unwrap()
}

/// Create new object store and poll future ad infinitum.
fn run<F, Fut>(f: F)
where
	F: Fn() -> Fut,
	Fut: Future<Output = ()>,
{
	let mut fut = core::pin::pin!(f());
	let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
	while fut.as_mut().poll(&mut cx).is_pending() {}
}

#[test]
fn create_fs() {
	run(|| async {
		new(MaxRecordSize::K1).await;
	})
}

#[test]
fn resize_object() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(1024).await.unwrap();
		obj.resize(2040).await.unwrap();
		obj.resize(1000).await.unwrap();
		obj.resize(0).await.unwrap();
	})
}

#[test]
fn write() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, &[0xcc; 1000]).await.unwrap();
	})
}

#[test]
fn finish_transaction() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, &[0xcc; 1000]).await.unwrap();
		s.finish_transaction().await.unwrap();
	})
}

#[test]
fn read_before_tx_offset_0() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();

		obj.resize(1000).await.unwrap();

		let l = obj.write(0, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let mut buf = [0; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);
	})
}

#[test]
fn read_before_tx_offset_1000() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();

		obj.resize(2000).await.unwrap();

		let l = obj.write(1000, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let mut buf = [0; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);

		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);
	})
}

#[test]
fn read_before_tx_offset_1023_short() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;

		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1023, b"Hello, world!").await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1023, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	})
}

#[test]
fn read_before_tx_offset_10p6() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();

		obj.resize(2_000_000).await.unwrap();

		let l = obj.write(1_000_000, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let mut buf = [0; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);

		obj.read(1_000_000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);
	})
}

#[test]
fn read_before_tx_offset_1000_short() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, b"Hello, world!").await.unwrap();
		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	})
}

#[test]
fn read_after_tx() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, &[1; 1000]).await.unwrap();
		s.finish_transaction().await.unwrap();
		let mut buf = [0; 1000];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);
		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);
	})
}

#[test]
fn read_before_tx_1024() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(1024).await.unwrap();
		obj.write(0, &[1; 1024]).await.unwrap();
		let mut buf = [0; 1024];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1024]);
	})
}

#[test]
fn replace_object() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;

		let obj_1 = s.create().await.unwrap();
		let obj_2 = s.create().await.unwrap();

		obj_2.resize(64).await.unwrap();
		obj_2.write(42, &[2; 2]).await.unwrap();

		obj_1.resize(2000).await.unwrap();
		obj_1.write(1000, &[1; 1000]).await.unwrap();

		let mut buf = [0; 1000];
		obj_1.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);
		obj_1.read(1000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);

		obj_1.replace_with(obj_2).await.unwrap();

		let mut buf = [0; 2];
		obj_1.read(42, &mut buf).await.unwrap();
		assert_eq!(buf, [2; 2]);
	})
}
