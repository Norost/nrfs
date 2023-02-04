pub mod fuzz;

mod allocator;
mod background;
mod cache;
mod concurrency;
mod dev;
mod encryption;
mod hard_limit;
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
	for _ in 0..1 << 18 {
		if let Poll::Ready(r) = fut.as_mut().poll(&mut cx) {
			return r;
		}
	}
	// Rust's async generation is pretty clever and shouldn't yield if it can do more work.
	// Since none of the test cases do any actual I/O this line shouldn't ever be reached.
	panic!("probably deadlocked?");
}

fn new_cap(
	max_record_size: MaxRecordSize,
	blocks: usize,
	cache_size: usize,
) -> Nros<MemDev, StdResource> {
	let s = MemDev::new(blocks, BlockSize::K1);
	let s = Nros::new(NewConfig {
		magic: *b"TEST",
		resource: StdResource::new(),
		mirrors: vec![vec![s]],
		block_size: BlockSize::K1,
		max_record_size,
		compression: Compression::None,
		cipher: CipherType::NoneXxh3,
		key_deriver: KeyDeriver::None { key: &[0; 32] },
		cache_size,
	});
	block_on(s).unwrap()
}

fn new(max_record_size: MaxRecordSize) -> Nros<MemDev, StdResource> {
	new_cap(max_record_size, 32, 4096)
}

fn run<D>(s: &Nros<D, StdResource>, fut: impl Future<Output = Result<(), Error<D>>>)
where
	D: Dev,
	D::Error: fmt::Debug,
{
	block_on(s.run(fut)).unwrap();
}

#[test]
fn create_fs() {
	new(MaxRecordSize::K1);
}

#[test]
fn create_destroy() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.decrease_reference_count().await.unwrap();

		Ok(())
	});
}

#[test]
#[should_panic]
fn create_destroy_twice() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.decrease_reference_count().await.unwrap();
		// Should panic here, as reference count is already zero
		obj.decrease_reference_count().await.unwrap();

		Ok(())
	});
}

#[test]
fn create_destroy_pair() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let id = s.create_many(2).await.unwrap();
		let obj_a = s.get(id + 0).await.unwrap();
		let obj_b = s.get(id + 1).await.unwrap();
		obj_a.decrease_reference_count().await.unwrap();
		obj_b.decrease_reference_count().await.unwrap();

		Ok(())
	});
}

#[test]
fn resize_object() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.resize(1024).await.unwrap();
		obj.resize(2040).await.unwrap();
		obj.resize(1000).await.unwrap();
		obj.resize(0).await.unwrap();

		Ok(())
	});
}

#[test]
fn write() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, &[0xcc; 1000]).await.unwrap();

		Ok(())
	});
}

#[test]
fn finish_transaction() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, &[0xcc; 1000]).await.unwrap();
		s.finish_transaction().await.unwrap();

		Ok(())
	});
}

#[test]
fn read_before_tx_offset_0() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();

		obj.resize(1000).await.unwrap();

		let l = obj.write(0, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let mut buf = [0; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);

		Ok(())
	});
}

#[test]
fn read_before_tx_offset_1000() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();

		obj.resize(2000).await.unwrap();

		let l = obj.write(1000, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let mut buf = [0; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);

		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);

		Ok(())
	});
}

#[test]
fn read_before_tx_offset_1023_short() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1023, b"Hello, world!").await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1023, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");

		Ok(())
	});
}

#[test]
fn read_before_tx_offset_10p6() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();

		obj.resize(2_000_000).await.unwrap();

		let l = obj.write(1_000_000, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let mut buf = [0; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);

		obj.read(1_000_000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);

		Ok(())
	});
}

#[test]
fn read_before_tx_offset_1000_short() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, b"Hello, world!").await.unwrap();
		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");

		Ok(())
	});
}

#[test]
fn read_after_tx() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, &[1; 1000]).await.unwrap();
		s.finish_transaction().await.unwrap();
		let mut buf = [0; 1000];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);
		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);

		Ok(())
	});
}

#[test]
fn read_before_tx_1024() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.resize(1024).await.unwrap();
		obj.write(0, &[1; 1024]).await.unwrap();
		let mut buf = [0; 1024];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1024]);

		Ok(())
	});
}

#[test]
fn replace_object() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
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

		Ok(())
	});
}
