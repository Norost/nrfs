pub mod fuzz;

mod allocator;
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
		obj.dealloc().await.unwrap();

		Ok(())
	});
}

#[test]
#[should_panic]
fn create_destroy_twice() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.dealloc().await.unwrap();
		// Should panic here, as reference count is already zero
		obj.dealloc().await.unwrap();

		Ok(())
	});
}

#[test]
fn write() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(1000, &[0xcc; 1000]).await.unwrap();

		Ok(())
	});
}

#[test]
fn finish_transaction() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
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
		obj.write(0, &[1; 1024]).await.unwrap();
		let mut buf = [0; 1024];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1024]);

		Ok(())
	});
}

#[test]
fn header_data() {
	let s = new(MaxRecordSize::K1);
	s.header_data()[..11].copy_from_slice(b"hello world");
	let devices = block_on(s.unmount()).unwrap();
	let s = block_on(Nros::load(LoadConfig {
		resource: StdResource::new(),
		devices,
		magic: *b"TEST",
		cache_size: 0,
		allow_repair: false,
		retrieve_key: &mut |_| todo!(),
	}))
	.unwrap();
	assert_eq!(&s.header_data()[..11], b"hello world");
}

#[test]
fn smaller_blocksize() {
	let s = MemDev::new(32, BlockSize::B512);
	let s = block_on(Nros::new(NewConfig {
		magic: *b"TEST",
		resource: StdResource::new(),
		mirrors: vec![vec![s]],
		block_size: BlockSize::K1,
		max_record_size: MaxRecordSize::K1,
		compression: Compression::None,
		cipher: CipherType::NoneXxh3,
		key_deriver: KeyDeriver::None { key: &[0; 32] },
		cache_size: 1 << 10,
	}))
	.unwrap();
	let devices = block_on(s.unmount()).unwrap();
	block_on(Nros::load(LoadConfig {
		magic: *b"TEST",
		resource: StdResource::new(),
		devices,
		cache_size: 1 << 10,
		allow_repair: false,
		retrieve_key: &mut |_| unreachable!(),
	}))
	.unwrap();
}
