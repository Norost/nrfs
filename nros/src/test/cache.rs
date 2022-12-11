use super::*;

/// Clear the entire cache of the object store.
async fn clear(s: &Nros<MemDev>) {
	s.resize_cache(0, 0).await.unwrap();
	let status = s.cache_status();
	assert_eq!(status.global_usage, 0);
	assert_eq!(status.dirty_usage, 0);
	s.resize_cache(4000, 4000).await.unwrap();
}

#[test]
fn create_flush_get() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let id = s.create().await.unwrap().id();
		clear(&s).await;
		s.get(id).await.unwrap();
	})
}

#[test]
fn write_flush_read() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;

		let obj = s.create().await.unwrap();
		obj.resize(1000).await.unwrap();
		obj.write(0, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	})
}

#[test]
fn write_flush_read_offset_1000() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;

		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	})
}

#[test]
fn write_flush_read_offset_1024() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;

		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1024, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&s).await;
		let obj = s.get(id).await.unwrap();
		assert_eq!(obj.len().await.unwrap(), 2000);

		let mut buf = [0; b"Hello, world!".len()];
		let l = obj.read(1024, &mut buf).await.unwrap();
		assert_eq!(l, buf.len());
		assert_eq!(&buf, b"Hello, world!");
	})
}

#[test]
fn write_flush_read_offset_1023() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		s.store.verify_cache_usage();

		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		s.store.verify_cache_usage();
		obj.write(1023, b"Hello, world!").await.unwrap();
		s.store.verify_cache_usage();

		let id = obj.id();
		clear(&s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1023, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	})
}

#[test]
fn write_flush_read_offset_10p6() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();

		obj.resize(2_000_000).await.unwrap();

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
	})
}

/// `new` sets global cache size to 4096, so this is guaranteed to cause evictions.
#[test]
fn write_read_2p13() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(1 << 13).await.unwrap();
		obj.write(0, &[1; 1 << 13]).await.unwrap();
		let mut buf = [0; 1 << 13];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1 << 13]);
	})
}

#[test]
fn write_tx_read_many() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;

		let obj_1 = s.create().await.unwrap();
		obj_1.resize(2000).await.unwrap();
		obj_1.write(1000, &[1; 1000]).await.unwrap();
		s.finish_transaction().await.unwrap();

		let obj_2 = s.create().await.unwrap();
		obj_2.resize(64).await.unwrap();
		obj_2.write(42, &[2; 2]).await.unwrap();
		s.finish_transaction().await.unwrap();

		let obj_3 = s.create().await.unwrap();
		obj_3.resize(1).await.unwrap();
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
	})
}

/// Ensure old records are properly disposed of.
///
/// `depth == 1`
#[test]
fn write_many_depth_eq1() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(1).await.unwrap();
		for _ in 0..1000 {
			obj.write(0, &[1]).await.unwrap();
			clear(&s).await;
		}
	})
}

/// Ensure old records are properly disposed of.
///
/// `depth > 1`
#[test]
fn write_many_depth_gt1() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(1 << 14).await.unwrap();
		for _ in 0..1000 {
			obj.write(0, &[1]).await.unwrap();
			clear(&s).await;
		}
	})
}

#[test]
fn shrink_written_object_0() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(1 << 20).await.unwrap();
		obj.write((1 << 20) - 1, &[1]).await.unwrap();
		clear(&s).await;
		obj.resize(0).await.unwrap();
	})
}

#[test]
fn shrink_written_object_1() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(1 << 20).await.unwrap();
		obj.write((1 << 20) - 1, &[1]).await.unwrap();
		obj.write((1 << 18) - 5, &[2]).await.unwrap();
		clear(&s).await;
		obj.resize((1 << 18) - 4).await.unwrap();
		let mut b = [0];
		obj.read((1 << 18) - 5, &mut b).await.unwrap();
		assert_eq!(b, [2]);
	})
}

#[test]
fn shrink_written_object_3() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(1 << 20).await.unwrap();
		obj.write((1 << 20) - 1, &[1]).await.unwrap();
		obj.write((1 << 18) - 5, &[2]).await.unwrap();
		clear(&s).await;
		obj.resize((1 << 18) - 6).await.unwrap();
		let mut b = [5];
		let l = obj.read((1 << 18) - 5, &mut b).await.unwrap();
		assert_eq!(l, 0);
		assert_eq!(b, [5]);
	})
}

#[test]
fn grow_grow() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;

		let obj = s.create().await.unwrap();

		obj.resize(1 << 15).await.unwrap();

		obj.write((1 << 15) - (1 << 10), &[1]).await.unwrap();

		obj.resize(1 << 25).await.unwrap();

		obj.write((1 << 25) - (1 << 10) - 1, &[2, 2]).await.unwrap();

		clear(&s).await;

		let mut b = [0; 1];
		obj.read((1 << 15) - (1 << 10), &mut b).await.unwrap();
		assert_eq!(b, [1]);

		let mut b = [0; 2];
		obj.read((1 << 25) - (1 << 10) - 1, &mut b).await.unwrap();
		assert_eq!(b, [2, 2]);
	})
}

#[test]
fn grow_write_shrink_many() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;

		let obj = s.create().await.unwrap();

		for i in 0..10 {
			obj.resize(1 << 60).await.unwrap();
			obj.write(0, &[1]).await.unwrap();
			clear(&s).await;
			obj.resize(0).await.unwrap();
			clear(&s).await;
		}
	})
}
