use {super::*, crate::header::Header};

/// Just create and save a filesystem with two devices.
#[test]
fn create_save_2() {
	run(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let s = Nros::new(
			StdResource::new(),
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
			StdResource::new(),
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

		let s = Nros::load(StdResource::new(), devs, 1 << 14, 1 << 14, true)
			.await
			.unwrap();
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
			StdResource::new(),
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
			s = Nros::load(StdResource::new(), devs, 1 << 14, 1 << 14, true)
				.await
				.unwrap();
			let obj = s.get(0).await.unwrap();
			let buf = &mut [0; 1 << 12];
			obj.read(0, buf).await.unwrap();
			assert_eq!(buf, &mut [1; 1 << 12]);
		}
	});
}

/// Corrupt the start headers only.
#[test]
fn corrupt_headers_2() {
	run(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let mut s = Nros::new(
			StdResource::new(),
			[[dev_a], [dev_b]],
			BlockSize::K1,
			MaxRecordSize::K16,
			Compression::None,
			1 << 14,
			1 << 14,
		)
		.await
		.unwrap();

		for i in 0..2 {
			for write_end in [false, true] {
				let devs = s.unmount().await.unwrap();

				// Read the header
				let mut header = Header::default();
				let header_len = header.as_ref().len();
				let lba = if write_end { (1 << 5) - 1 } else { 0 };
				let mut buf = devs[i].read(lba, 1 << 10).await.unwrap();
				header.as_mut().copy_from_slice(&buf.get()[..header_len]);

				// Make the hash of the header invalid.
				header.xxh3 = !header.xxh3;

				// Corrupt the header.
				buf.get_mut()[..header_len].copy_from_slice(header.as_ref());
				devs[i].write(lba, buf).await.unwrap();

				// Try to remount
				// The filesystem should be automatically repaired.
				s = Nros::load(StdResource::new(), devs, 1 << 14, 1 << 14, true)
					.await
					.unwrap();
			}
		}
	})
}
