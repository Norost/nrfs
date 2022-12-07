use super::*;

/// Clear the entire cache of the object store.
async fn clear(s: &mut Nros<MemDev>) {
	s.resize_cache(0, 0).await.unwrap();
	let status = s.cache_status();
	assert_eq!(status.global_usage, 0);
	assert_eq!(status.dirty_usage, 0);
	s.resize_cache(4000, 4000).await.unwrap();
}

#[test]
fn create_flush_get() {
	run(|| async {
		let mut s = new(MaxRecordSize::K1).await;
		let id = s.create().await.unwrap().id();
		clear(&mut s).await;
		s.get(id).await.unwrap();
	})
}

#[test]
fn write_flush_read() {
	run(|| async {
		let mut s = new(MaxRecordSize::K1).await;

		let obj = s.create().await.unwrap();
		obj.resize(1000).await.unwrap();
		obj.write(0, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&mut s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	})
}

#[test]
fn write_flush_read_offset_1000() {
	run(|| async {
		let mut s = new(MaxRecordSize::K1).await;

		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1000, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&mut s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1000, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	})
}

#[test]
fn write_flush_read_offset_1024() {
	run(|| async {
		let mut s = new(MaxRecordSize::K1).await;

		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		obj.write(1024, b"Hello, world!").await.unwrap();

		let id = obj.id();
		clear(&mut s).await;
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
		let mut s = new(MaxRecordSize::K1).await;
		s.store.verify_cache_usage();

		let obj = s.create().await.unwrap();
		obj.resize(2000).await.unwrap();
		s.store.verify_cache_usage();
		obj.write(1023, b"Hello, world!").await.unwrap();
		s.store.verify_cache_usage();

		let id = obj.id();
		clear(&mut s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [0; b"Hello, world!".len()];
		obj.read(1023, &mut buf).await.unwrap();
		assert_eq!(&buf, b"Hello, world!");
	})
}

#[test]
fn write_flush_read_offset_10p6() {
	run(|| async {
		let mut s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();

		obj.resize(2_000_000).await.unwrap();

		let l = obj.write(1_000_000, &[1; 1000]).await.unwrap();
		assert_eq!(l, 1000);

		let id = obj.id();
		clear(&mut s).await;
		let obj = s.get(id).await.unwrap();

		let mut buf = [2; 1000];

		obj.read(0, &mut buf).await.unwrap();
		assert_eq!(buf, [0; 1000]);

		obj.read(1_000_000, &mut buf).await.unwrap();
		assert_eq!(buf, [1; 1000]);
	})
}
