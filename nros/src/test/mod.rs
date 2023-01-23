pub mod fuzz;

mod allocator;
mod background;
mod cache;
mod concurrency;
mod dev;
mod raid;
mod record;

use {
	crate::{dev::*, *},
	core::{
		future::Future,
		task::{Context, Poll},
	},
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

fn new_cap(
	max_record_size: MaxRecordSize,
	blocks: usize,
	cache_size: usize,
) -> Nros<MemDev, StdResource> {
	let s = MemDev::new(blocks, BlockSize::K1);
	let s = Nros::new(
		StdResource::new(),
		[[s]],
		BlockSize::K1,
		max_record_size,
		Compression::None,
		cache_size,
	);
	block_on(s).unwrap()
}

fn new(max_record_size: MaxRecordSize) -> Nros<MemDev, StdResource> {
	new_cap(max_record_size, 32, 4096)
}

/// Create new object store and poll future ad infinitum.
fn run2<'a, D, F>(bg: &Background<'a, D>, f: F)
where
	F: Future<Output = ()>,
	D: Dev,
	D::Error: core::fmt::Debug,
{
	block_on(bg.run(async { Ok::<_, Error<D>>(f.await) })).unwrap();
}

#[test]
fn create_fs() {
	new(MaxRecordSize::K1);
}

#[test]
fn create_destroy() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.decrease_reference_count().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
#[should_panic]
fn create_destroy_twice() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.decrease_reference_count().await.unwrap();
		// Should panic here, as reference count is already zero
		obj.decrease_reference_count().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn create_destroy_pair() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let id = s.create_many(&bg, 2).await.unwrap();
		let obj_a = s.get(&bg, id + 0).await.unwrap();
		let obj_b = s.get(&bg, id + 1).await.unwrap();
		obj_a.decrease_reference_count().await.unwrap();
		obj_b.decrease_reference_count().await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn resize_object() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1024).await.unwrap();
		obj.resize(2040).await.unwrap();
		obj.resize(1000).await.unwrap();
		obj.resize(0).await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn write() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, &[0xcc; 1000]).await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn finish_transaction() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, &[0xcc; 1000]).await.unwrap();
		s.finish_transaction(&bg).await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn read_before_tx_offset_0() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();

		obj.resize(1000).await.unwrap();

		let l = obj.write(0, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let mut buf = [0; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn read_before_tx_offset_1000() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();

		obj.resize(2000).await.unwrap();

		let l = obj.write(1000, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let mut buf = [0; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);

		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn read_before_tx_offset_1023_short() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1023, b"Hello, world!").await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1023, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn read_before_tx_offset_10p6() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();

		obj.resize(2_000_000).await.unwrap();

		let l = obj.write(1_000_000, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let mut buf = [0; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);

		obj.read(1_000_000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn read_before_tx_offset_1000_short() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, b"Hello, world!").await.unwrap();
		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn read_after_tx() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, &[1; 1000]).await.unwrap();
		s.finish_transaction(&bg).await.unwrap();
		let mut buf = [0; 1000];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);
		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn read_before_tx_1024() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1024).await.unwrap();
		obj.write(0, &[1; 1024]).await.unwrap();
		let mut buf = [0; 1024];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1024]);
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn replace_object() {
	let s = new(MaxRecordSize::K1);
	let bg = Default::default();
	run2(&bg, async {
		let obj_1 = s.create(&bg).await.unwrap();
		let obj_2 = s.create(&bg).await.unwrap();

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
	});
	block_on(bg.drop()).unwrap();
}
