use super::*;

/// Just create and save a filesystem with two devices.
#[test]
fn create_save_2() {
	run(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let s = Nros::new(
			[[dev_a, dev_b]],
			BlockSize::K1,
			MaxRecordSize::K16,
			Compression::None,
			1 << 14,
			1 << 14,
		)
		.await
		.unwrap();
		s.unmount().await.unwrap();
	});
}

/// Test with two equally-sized devices and fill up to just above 1x the limit of one device.
///
/// At least one record should end up crossing the boundary between the two devices.
/// This can be verified with `cargo llvm-cov`.
#[test]
fn equal_2() {
	run(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let s = Nros::new(
			[[dev_a, dev_b]],
			BlockSize::K1,
			MaxRecordSize::K16,
			Compression::None,
			1 << 14,
			1 << 14,
		)
		.await
		.unwrap();

		let obj = s.create().await.unwrap();
		obj.resize(1 << 15).await.unwrap();
		obj.write(0, &[1; 1 << 15]).await.unwrap();
		drop(obj);
		let devs = s.unmount().await.unwrap();

		let s = Nros::load(devs, 1 << 14, 1 << 14, true).await.unwrap();
		let obj = s.get(0).await.unwrap();
		let buf = &mut [0; 1 << 15];
		obj.read(0, buf).await.unwrap();
		assert_eq!(buf, &mut [1; 1 << 15]);
	});
}
