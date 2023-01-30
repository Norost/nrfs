use super::*;

/// Just create and save a filesystem with two devices.
#[test]
fn create_save_2() {
	block_on(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let s = new(vec![vec![dev_a, dev_b]]).await;
		s.unmount().await.unwrap();
	});
}

/// Test with two equally-sized devices and fill up to just above 1x the limit of one device.
///
/// At least one record should end up crossing the boundary between the two devices.
/// This can be verified with `cargo llvm-cov`.
#[test]
fn equal_2() {
	block_on(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let s = new(vec![vec![dev_a, dev_b]]).await;
		let bg = Background::default();

		let obj = s.create(&bg).await.unwrap();
		obj.resize(1 << 15).await.unwrap();
		obj.write(0, &[1; 1 << 15]).await.unwrap();
		drop(obj);

		bg.try_run_all().await.unwrap();
		bg.drop().await.unwrap();

		let devs = s.unmount().await.unwrap();

		let s = load(devs).await;
		let bg = Background::default();
		let obj = s.get(&bg, 0).await.unwrap();
		let buf = &mut [0; 1 << 15];
		obj.read(0, buf).await.unwrap();
		assert_eq!(buf, &mut [1; 1 << 15]);
		bg.drop().await.unwrap();
	});
}
