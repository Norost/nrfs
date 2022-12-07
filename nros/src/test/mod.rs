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
		let mut s = new(MaxRecordSize::K1).await;
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
		let mut s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, &[0xcc; 1000]).await.unwrap();
	})
}

#[test]
fn finish_transaction() {
	run(|| async {
		let mut s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, &[0xcc; 1000]).await.unwrap();
		s.finish_transaction().await.unwrap();
	})
}

#[test]
fn read_before_tx_offset_0() {
	run(|| async {
		let mut s = new(MaxRecordSize::K1).await;
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
		let mut s = new(MaxRecordSize::K1).await;
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
fn read_before_tx_offset_10p6() {
	run(|| async {
		let mut s = new(MaxRecordSize::K1).await;
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
		let mut s = new(MaxRecordSize::K1).await;
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
		let mut s = new(MaxRecordSize::K1).await;
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

/*
#[test]
fn write_tx_read_many() {
	let mut s = new(MaxRecordSize::K1);

	let obj = s.new_object().unwrap();
	s.resize(id, 2000).unwrap();
	s.write(id, 1000, &[0xcc; 1000]).unwrap();
	s.finish_transaction().unwrap();

	let id2 = s.new_object().unwrap();
	s.resize(id2, 64).unwrap();
	s.write(id2, 42, &[0xde; 2]).unwrap();
	s.finish_transaction().unwrap();

	let id3 = s.new_object().unwrap();
	s.resize(id3, 1).unwrap();
	s.write(id3, 0, &[1]).unwrap();
	s.finish_transaction().unwrap();

	let mut buf = [0; 1000];
	s.read(id, 0, &mut buf).unwrap();
	assert_eq!(buf, [0; 1000]);
	s.read(id, 1000, &mut buf).unwrap();
	assert_eq!(buf, [0xcc; 1000]);

	let mut buf = [0; 2];
	s.read(id2, 42, &mut buf).unwrap();
	assert_eq!(buf, [0xde; 2]);

	let mut buf = [0];
	s.read(id3, 0, &mut buf).unwrap();
	assert_eq!(buf, [1]);
}

#[test]
fn write_new_write() {
	let mut s = new(MaxRecordSize::K1);

	let obj = s.new_object().unwrap();
	let id2 = s.new_object().unwrap();

	s.resize(id2, 64).unwrap();
	s.write(id2, 42, &[0xde; 2]).unwrap();

	s.resize(id, 2000).unwrap();
	s.write(id, 1000, &[0xcc; 1000]).unwrap();

	let mut buf = [0; 1000];
	s.read(id, 0, &mut buf).unwrap();
	assert_eq!(buf, [0; 1000]);
	s.read(id, 1000, &mut buf).unwrap();
	assert_eq!(buf, [0xcc; 1000]);

	s.move_object(id, id2).unwrap();

	let mut buf = [0; 2];
	s.read(id, 42, &mut buf).unwrap();
	assert_eq!(buf, [0xde; 2]);
}
*/
