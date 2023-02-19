use super::*;

/// Clear the entire cache of the object store.
async fn clear<'a, 'b>(s: &'a Nros<MemDev, StdResource>) {
	s.resize_cache(0).unwrap();
	// FIXME wait for evicts to finish or something
	//assert_eq!(s.statistics().soft_usage, 0);
	s.resize_cache(4000).unwrap();
}

#[test]
fn create_flush_get() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let id = s.create().await.unwrap().id();
		clear(&s).await;
		s.get(id).await.unwrap();

		Ok(())
	});
}

#[test]
fn write_flush_read_offset_0() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(0, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");

		Ok(())
	});
}

#[test]
fn write_flush_read_offset_1000() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(1000, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");

		Ok(())
	});
}

#[test]
fn write_flush_read_offset_1024() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(1024, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		let l = obj.read(1024, &mut buf).await.unwrap();
		assert_eq!(l, buf.len());
		assert_eq!(&buf, b"Hello, world!");

		Ok(())
	});
}

#[test]
fn write_flush_read_offset_1023() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		s.store.verify_cache_usage();

		let obj = s.create().await.unwrap();
		s.store.verify_cache_usage();
		obj.write(1023, b"Hello, world!").await.unwrap();
		s.store.verify_cache_usage();

		let id = obj.id();
		clear(&s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1023, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");

		Ok(())
	});
}

#[test]
fn write_flush_read_offset_10p6() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();

		let l = obj.write(1_000_000, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let id = obj.id();
		clear(&s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [2; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);

		obj.read(1_000_000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);

		Ok(())
	});
}

/// `new` sets global cache size to 4096, so this is guaranteed to cause evictions.
#[test]
fn write_read_2p13() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(0, &[1; 1 << 13]).await.unwrap();
		let mut buf = [0; 1 << 13];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1 << 13]);

		Ok(())
	});
}

#[test]
fn write_tx_read_many() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj_1 = s.create().await.unwrap();
		obj_1.write(1000, &[1; 1000]).await.unwrap();
		s.finish_transaction().await.unwrap();

		let obj_2 = s.create().await.unwrap();
		obj_2.write(42, &[2; 2]).await.unwrap();
		s.finish_transaction().await.unwrap();

		let obj_3 = s.create().await.unwrap();
		obj_3.write(0, &[3]).await.unwrap();
		s.finish_transaction().await.unwrap();

		let mut buf = [0; 1000];
		obj_1.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);
		obj_1.read(1000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);

		let mut buf = [0; 2];
		obj_2.read(42, &mut buf).await.unwrap();
		assert_eq!(buf, [2; 2]);

		let mut buf = [0];
		obj_3.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [3]);

		Ok(())
	});
}

/// Ensure old records are properly disposed of.
///
/// `depth == 1`
#[test]
fn write_many_depth_eq1() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		for _ in 0..1000 {
			obj.write(0, &[1]).await.unwrap();
			clear(&s).await;
		}

		Ok(())
	});
}

/// Ensure old records are properly disposed of.
///
/// `depth > 1`
#[test]
fn write_many_depth_gt1() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		for _ in 0..1000 {
			obj.write(0, &[1]).await.unwrap();
			clear(&s).await;
		}

		Ok(())
	});
}

#[test]
fn write_flush_many() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();

		for _ in 0..200 {
			obj.write(0, &[1]).await.unwrap();
			clear(&s).await;
		}

		Ok(())
	});
}

/// Zero out an object with a depth of one.
#[test]
fn write_zeros_all_small() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(0, &[1; 1024]).await.unwrap();

		obj.write_zeros(0, 1024).await.unwrap();
		let buf = &mut [1; 1024];
		obj.read(0, buf).await.unwrap();
		assert_eq!(buf, &[0; 1024]);

		Ok(())
	});
}

/// Zero out left half of an object with a depth of one.
#[test]
fn write_zeros_left_small() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(0, &[1; 1024]).await.unwrap();

		obj.write_zeros(0, 512).await.unwrap();
		let buf = &mut [2; 1024];
		obj.read(0, buf).await.unwrap();
		assert_eq!(&buf[..512], &[0; 512]);
		assert_eq!(&buf[512..], &[1; 512]);

		Ok(())
	});
}

/// Zero out right half of an object with a depth of one.
#[test]
fn write_zeros_right_small() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(0, &[1; 1024]).await.unwrap();

		obj.write_zeros(512, 512).await.unwrap();
		let buf = &mut [2; 1024];
		obj.read(0, buf).await.unwrap();
		assert_eq!(&buf[..512], &[1; 512]);
		assert_eq!(&buf[512..], &[0; 512]);

		Ok(())
	});
}

/// Zero out an object with a depth >1.
#[test]
fn write_zeros_packed_large() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(0, &[1; 1 << 12]).await.unwrap();

		obj.write_zeros(0, 1 << 50).await.unwrap();
		let buf = &mut [2; 1 << 12];
		obj.read(0, buf).await.unwrap();
		assert_eq!(buf, &[0; 1 << 12]);

		Ok(())
	});
}

/// Zero out an object with a depth >1.
#[test]
fn write_zeros_spread_large() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(1 << 10, &[1; 1 << 10]).await.unwrap();
		obj.write(1 << 20, &[1; 1 << 10]).await.unwrap();
		obj.write(1 << 30, &[1; 1 << 10]).await.unwrap();

		obj.write_zeros(0, 1 << 50).await.unwrap();

		let buf = &mut [2; 1 << 10];
		obj.read(1 << 10, buf).await.unwrap();
		assert_eq!(buf, &[0; 1 << 10]);

		let buf = &mut [2; 1 << 10];
		obj.read(1 << 20, buf).await.unwrap();
		assert_eq!(buf, &[0; 1 << 10]);

		let buf = &mut [2; 1 << 10];
		obj.read(1 << 30, buf).await.unwrap();
		assert_eq!(buf, &[0; 1 << 10]);

		Ok(())
	});
}

#[test]
fn write_zeros_1() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();

		obj.write((1 << 18) - 5, &[2]).await.unwrap();

		clear(&s).await;

		obj.write_zeros((1 << 18) - 4, u64::MAX).await.unwrap();

		let mut b = [0];
		let l = obj.read((1 << 18) - 5, &mut b).await.unwrap();
		assert_eq!(l, 1);
		assert_eq!(b, [2]);

		Ok(())
	});
}

/// Ensure resizing object list & bitmap works properly.
#[test]
fn create_many() {
	let s = new_cap(MaxRecordSize::K1, 1 << 16, 1 << 10);
	run(&s, async {
		for _ in 0..1024 * 8 + 1 {
			s.create().await.unwrap();
		}
		Ok(())
	});
	block_on(s.unmount()).unwrap();
}
