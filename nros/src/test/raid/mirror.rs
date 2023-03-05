use {super::*, crate::data::fs_info::FsHeader};

/// Just create and save a filesystem with two devices.
#[test]
fn create_save_2() {
	block_on(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let s = new(vec![vec![dev_a], vec![dev_b]]).await;
		s.unmount().await.unwrap();
	});
}

#[test]
fn write_read_2() {
	block_on(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let s = new(vec![vec![dev_a], vec![dev_b]]).await;

		let obj = s.create().await.unwrap();
		obj.write(0, &[1; 1 << 12]).await.unwrap();
		drop(obj);

		let devs = s.unmount().await.unwrap();

		let s = load(devs).await;

		let obj = s.get(0);
		let buf = &mut [0; 1 << 12];
		obj.read(0, buf).await.unwrap();
		assert_eq!(buf, &mut [1; 1 << 12]);
	});
}

/// Check if recovery works with *all* data except headers are broken on one device.
#[test]
fn write_corrupt_read_2() {
	block_on(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let mut s = new(vec![vec![dev_a], vec![dev_b]]).await;

		let obj = s.create().await.unwrap();
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
			s = load(devs).await;

			let obj = s.get(0);
			let buf = &mut [0; 1 << 12];
			obj.read(0, buf).await.unwrap();

			assert_eq!(buf, &mut [1; 1 << 12]);
		}
	});
}

/// Corrupt the start headers only.
#[test]
fn corrupt_headers_2() {
	block_on(async {
		let dev_a = dev::MemDev::new(1 << 5, BlockSize::K1);
		let dev_b = dev::MemDev::new(1 << 5, BlockSize::K1);
		let mut s = new(vec![vec![dev_a], vec![dev_b]]).await;

		for i in 0..2 {
			for write_end in [false, true] {
				let devs = s.unmount().await.unwrap();

				// Read the header
				let lba = if write_end { (1 << 5) - 1 } else { 0 };
				let mut buf = devs[i].read(lba, 1 << 10).await.unwrap();
				let (mut header, _) = FsHeader::from_raw_slice(buf.get()).unwrap();

				// Make the hash of the header invalid.
				header.hash.iter_mut().for_each(|x| *x = !*x);

				// Corrupt the header.
				buf.get_mut()[..header.as_ref().len()].copy_from_slice(header.as_ref());
				devs[i].write(lba, buf).await.unwrap();

				// Try to remount
				// The filesystem should be automatically repaired.
				s = load(devs).await;
			}
		}
	})
}
