use super::*;

/// Clear the entire cache of the object store.
async fn clear<'a, 'b>(s: &'a Nros<MemDev, StdResource>, bg: &'b Background<'a, MemDev>) {
	s.resize_cache(&bg, 0).await.unwrap();
	let stat = s.statistics();
	assert_eq!(stat.global_usage, 0);
	s.resize_cache(&bg, 4000).await.unwrap();
}

#[test]
fn create_flush_get() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let id = s.create(&bg).await.unwrap().id();
		clear(&s, &bg).await;
		s.get(&bg, id).await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn write_flush_read_offset_0() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1000).await.unwrap();
		obj.write(0, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&s, &bg).await;
		let obj = s.get(&bg, id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn write_flush_read_offset_1000() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&s, &bg).await;
		let obj = s.get(&bg, id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn write_flush_read_offset_1024() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1024, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&s, &bg).await;
		let obj = s.get(&bg, id).await.unwrap();
		assert_eq!(obj.len().await.unwrap(), 2000);

		let mut buf = [0; b"Hello, world!".len()];
		let l = obj.read(1024, &mut buf).await.unwrap();
		assert_eq!(l, buf.len());
		assert_eq!(&buf, b"Hello, world!");
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn write_flush_read_offset_1023() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		s.store.verify_cache_usage();

		let obj = s.create(&bg).await.unwrap();
		obj.resize(2000).await.unwrap();
		s.store.verify_cache_usage();
		obj.write(1023, b"Hello, world!").await.unwrap();
		s.store.verify_cache_usage();

		let id = obj.id();
		clear(&s, &bg).await;
		let obj = s.get(&bg, id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1023, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn write_flush_read_offset_10p6() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();

		obj.resize(2_000_000).await.unwrap();

		let l = obj.write(1_000_000, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let id = obj.id();
		clear(&s, &bg).await;
		let obj = s.get(&bg, id).await.unwrap();

		let mut buf = [2; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);

		obj.read(1_000_000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);
	});
	block_on(bg.drop()).unwrap();
}

/// `new` sets global cache size to 4096, so this is guaranteed to cause evictions.
#[test]
fn write_read_2p13() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1 << 13).await.unwrap();
		obj.write(0, &[1; 1 << 13]).await.unwrap();
		let mut buf = [0; 1 << 13];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1 << 13]);
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn write_tx_read_many() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj_1 = s.create(&bg).await.unwrap();
		obj_1.resize(2000).await.unwrap();
		obj_1.write(1000, &[1; 1000]).await.unwrap();
		s.finish_transaction(&bg).await.unwrap();

		let obj_2 = s.create(&bg).await.unwrap();
		obj_2.resize(64).await.unwrap();
		obj_2.write(42, &[2; 2]).await.unwrap();
		s.finish_transaction(&bg).await.unwrap();

		let obj_3 = s.create(&bg).await.unwrap();
		obj_3.resize(1).await.unwrap();
		obj_3.write(0, &[3]).await.unwrap();
		s.finish_transaction(&bg).await.unwrap();

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
	});
	block_on(bg.drop()).unwrap();
}

/// Ensure old records are properly disposed of.
///
/// `depth == 1`
#[test]
fn write_many_depth_eq1() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1).await.unwrap();
		for _ in 0..1000 {
			obj.write(0, &[1]).await.unwrap();
			clear(&s, &bg).await;
		}
	});
	block_on(bg.drop()).unwrap();
}

/// Ensure old records are properly disposed of.
///
/// `depth > 1`
#[test]
fn write_many_depth_gt1() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1 << 14).await.unwrap();
		for _ in 0..1000 {
			obj.write(0, &[1]).await.unwrap();
			clear(&s, &bg).await;
		}
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn shrink_written_object_0() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1 << 20).await.unwrap();
		obj.write((1 << 20) - 1, &[1]).await.unwrap();
		clear(&s, &bg).await;
		obj.resize(0).await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn shrink_written_object_1() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();

		obj.resize(1 << 20).await.unwrap();
		obj.write((1 << 18) - 5, &[2]).await.unwrap();

		clear(&s, &bg).await;

		obj.resize((1 << 18) - 4).await.unwrap();

		let mut b = [0];
		let l = obj.read((1 << 18) - 5, &mut b).await.unwrap();
		assert_eq!(l, 1);
		assert_eq!(b, [2]);
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn shrink_written_object_2() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1 << 20).await.unwrap();
		obj.write((1 << 20) - 1, &[1]).await.unwrap();
		obj.write((1 << 18) - 5, &[2]).await.unwrap();
		clear(&s, &bg).await;
		obj.resize((1 << 18) - 4).await.unwrap();
		let mut b = [0];
		let l = obj.read((1 << 18) - 5, &mut b).await.unwrap();
		assert_eq!(l, 1);
		assert_eq!(b, [2]);
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn shrink_written_object_3() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1 << 20).await.unwrap();
		obj.write((1 << 20) - 1, &[1]).await.unwrap();
		obj.write((1 << 18) - 5, &[2]).await.unwrap();
		clear(&s, &bg).await;
		obj.resize((1 << 18) - 6).await.unwrap();
		let mut b = [5];
		let l = obj.read((1 << 18) - 5, &mut b).await.unwrap();
		// We shouldn't have read anything
		assert_eq!(l, 0);
		assert_eq!(b, [5]);
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn grow_grow() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();

		obj.resize(1 << 15).await.unwrap();

		obj.write((1 << 15) - (1 << 10), &[1]).await.unwrap();

		obj.resize(1 << 25).await.unwrap();

		obj.write((1 << 25) - (1 << 10) - 1, &[2, 2]).await.unwrap();

		clear(&s, &bg).await;

		let mut b = [0; 1];
		obj.read((1 << 15) - (1 << 10), &mut b).await.unwrap();
		assert_eq!(b, [1]);

		let mut b = [0; 2];
		obj.read((1 << 25) - (1 << 10) - 1, &mut b).await.unwrap();
		assert_eq!(b, [2, 2]);
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn grow_write_shrink_flush_many() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();

		for _ in 0..200 {
			obj.resize(1 << 60).await.unwrap();
			obj.write(0, &[1]).await.unwrap();
			obj.resize(0).await.unwrap();
			clear(&s, &bg).await;
		}
	});
	block_on(bg.drop()).unwrap();
}

/// Depth = 1
#[test]
fn grow_write_flush_shrink_flush_many_d1() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();

		for _ in 0..200 {
			obj.resize(1).await.unwrap();
			obj.write(0, &[1]).await.unwrap();
			clear(&s, &bg).await;
			obj.resize(0).await.unwrap();
			clear(&s, &bg).await;
		}
	});
	block_on(bg.drop()).unwrap();
}

/// Depth = 2
#[test]
fn grow_write_flush_shrink_flush_many_d2() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();

		for _ in 0..200 {
			obj.resize((1 << 10) + 1).await.unwrap();
			obj.write(0, &[1]).await.unwrap();
			clear(&s, &bg).await;
			obj.resize(0).await.unwrap();
			clear(&s, &bg).await;
		}
	});
	block_on(bg.drop()).unwrap();
}

/// Depth > 2
#[test]
fn grow_write_flush_shrink_flush_many_deep() {
	let s = new_cap(MaxRecordSize::K1, 64, 4096);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();

		for _ in 0..200 {
			obj.resize(1 << 60).await.unwrap();
			obj.write(0, &[1]).await.unwrap();
			clear(&s, &bg).await;
			obj.resize(0).await.unwrap();
			clear(&s, &bg).await;
		}
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn write_flush_many() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1).await.unwrap();

		for _ in 0..200 {
			obj.write(0, &[1]).await.unwrap();
			clear(&s, &bg).await;
		}
	});
	block_on(bg.drop()).unwrap();
}

/// Zero out an object with a depth of one.
#[test]
fn write_zeros_all_small() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1024).await.unwrap();
		obj.write(0, &[1; 1024]).await.unwrap();

		obj.write_zeros(0, 1024).await.unwrap();
		let buf = &mut [1; 1024];
		obj.read(0, buf).await.unwrap();
		assert_eq!(buf, &[0; 1024]);
	});
	block_on(bg.drop()).unwrap();
}

/// Zero out left half of an object with a depth of one.
#[test]
fn write_zeros_left_small() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1024).await.unwrap();
		obj.write(0, &[1; 1024]).await.unwrap();

		obj.write_zeros(0, 512).await.unwrap();
		let buf = &mut [2; 1024];
		obj.read(0, buf).await.unwrap();
		assert_eq!(&buf[..512], &[0; 512]);
		assert_eq!(&buf[512..], &[1; 512]);
	});
	block_on(bg.drop()).unwrap();
}

/// Zero out right half of an object with a depth of one.
#[test]
fn write_zeros_right_small() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1024).await.unwrap();
		obj.write(0, &[1; 1024]).await.unwrap();

		obj.write_zeros(512, 512).await.unwrap();
		let buf = &mut [2; 1024];
		obj.read(0, buf).await.unwrap();
		assert_eq!(&buf[..512], &[1; 512]);
		assert_eq!(&buf[512..], &[0; 512]);
	});
	block_on(bg.drop()).unwrap();
}

/// Zero out an object with a depth >1.
#[test]
fn write_zeros_packed_large() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1 << 50).await.unwrap();
		obj.write(0, &[1; 1 << 12]).await.unwrap();

		obj.write_zeros(0, 1 << 50).await.unwrap();
		let buf = &mut [1; 1 << 12];
		obj.read(0, buf).await.unwrap();
		assert_eq!(buf, &[0; 1 << 12]);
	});
	block_on(bg.drop()).unwrap();
}

/// Zero out an object with a depth >1.
#[test]
fn write_zeros_spread_large() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1 << 50).await.unwrap();
		obj.write(1 << 10, &[1; 1 << 10]).await.unwrap();
		obj.write(1 << 20, &[1; 1 << 10]).await.unwrap();
		obj.write(1 << 30, &[1; 1 << 10]).await.unwrap();
		obj.write(1 << 40, &[1; 1 << 10]).await.unwrap();

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

		let buf = &mut [2; 1 << 10];
		obj.read(1 << 40, buf).await.unwrap();
		assert_eq!(buf, &[0; 1 << 10]);
	});
	block_on(bg.drop()).unwrap();
}

#[test]
fn write_zeros_1() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();

		obj.resize(1 << 20).await.unwrap();
		obj.write((1 << 18) - 5, &[2]).await.unwrap();

		clear(&s, &bg).await;

		obj.write_zeros((1 << 18) - 4, u64::MAX).await.unwrap();

		let mut b = [0];
		let l = obj.read((1 << 18) - 5, &mut b).await.unwrap();
		assert_eq!(l, 1);
		assert_eq!(b, [2]);
	});
	block_on(bg.drop()).unwrap();
}

/// Ensure resizing object list & bitmap works properly.
#[test]
fn create_many() {
	let s = new_cap(MaxRecordSize::K1, 1 << 16, 1 << 10);
	let bg = Background::default();
	run2(&bg, async {
		for _ in 0..1024 * 4 + 1 {
			s.create(&bg).await.unwrap();
		}
	});
	block_on(bg.drop()).unwrap();
	block_on(s.unmount()).unwrap();
}

/// Ensure creating a large amount of objects at once with `create_many` works properly.
#[test]
fn create_many_batch() {
	let s = new_cap(MaxRecordSize::K1, 1 << 16, 1 << 10);
	let bg = Background::default();
	run2(&bg, async {
		s.create_many(&bg, 1024 * 4 + 1).await.unwrap();
	});
	block_on(bg.drop()).unwrap();
	block_on(s.unmount()).unwrap();
}
