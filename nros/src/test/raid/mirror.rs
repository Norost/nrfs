use super::*;

/// Just create and save a filesystem with two devices.
#[test]
fn create_save_2() {
	run(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let s = Nros::new(
			[[dev_a], [dev_b]],
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

#[test]
fn write_read_2() {
	run(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let s = Nros::new(
			[[dev_a], [dev_b]],
			BlockSize::K1,
			MaxRecordSize::K16,
			Compression::None,
			1 << 14,
			1 << 14,
		)
		.await
		.unwrap();

		let obj = s.create().await.unwrap();
		obj.resize(1 << 12).await.unwrap();
		obj.write(0, &[1; 1 << 12]).await.unwrap();
		drop(obj);
		let devs = s.unmount().await.unwrap();

		let s = Nros::load(devs, 1 << 14, 1 << 14).await.unwrap();
		let obj = s.get(0).await.unwrap();
		let buf = &mut [0; 1 << 12];
		obj.read(0, buf).await.unwrap();
		assert_eq!(buf, &mut [1; 1 << 12]);
	});
}

/// Check if recovery works with *all* data except headers are broken on one device.
#[test]
fn write_corrupt_read_2() {
	run(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let mut s = Nros::new(
			[[dev_a], [dev_b]],
			BlockSize::K1,
			MaxRecordSize::K16,
			Compression::None,
			1 << 14,
			1 << 14,
		)
		.await
		.unwrap();

		let obj = s.create().await.unwrap();
		obj.resize(1 << 12).await.unwrap();
		obj.write(0, &[1; 1 << 12]).await.unwrap();
		drop(obj);

		for i in 0..2 {
			let devs = s.unmount().await.unwrap();

			// Wipe a device except headers.
			let mut buf = devs[i]
				.allocator()
				.alloc(((1 << 5) - 2) << 10)
				.await
				.unwrap();
			buf.get_mut().fill(9);
			devs[i].write(1, buf).await.unwrap();

			// Remount & test
			s = Nros::load(devs, 1 << 14, 1 << 14).await.unwrap();
			let obj = s.get(0).await.unwrap();
			let buf = &mut [0; 1 << 12];
			obj.read(0, buf).await.unwrap();
			assert_eq!(buf, &mut [1; 1 << 12]);
		}
	});
}
