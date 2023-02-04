use {super::*, crate::dev};

async fn load(devices: Vec<dev::MemDev>) -> Nros<dev::MemDev, StdResource> {
	Nros::load(LoadConfig {
		magic: *b"TEST",
		resource: StdResource::new(),
		devices,
		cache_size: 1 << 12,
		retrieve_key: &mut |_| unreachable!(),
		allow_repair: true,
	})
	.await
	.unwrap()
}

/// Check if an object store is correctly saved before unmounting.
/// And also whether loading works.
#[test]
fn write_remount_read() {
	let s = new(MaxRecordSize::K1);

	let id = block_on(s.run(async {
		let obj = s.create().await.unwrap();
		obj.resize(1).await.unwrap();
		obj.write(0, &[1]).await.unwrap();
		Ok::<_, Error<_>>(obj.id())
	}))
	.unwrap();

	let s = block_on(async {
		let devs = s.unmount().await.unwrap();
		load(devs).await
	});

	block_on(s.run(async {
		let obj = s.get(id).await.unwrap();
		let mut buf = [0];
		let l = obj.read(0, &mut buf).await.unwrap();
		assert_eq!(l, 1);
		assert_eq!(buf, [1]);
		Ok::<_, Error<_>>(())
	}))
	.unwrap();
}

/// Check if an object store is correctly saved before unmounting.
/// And also whether loading works.
#[test]
fn write_remount_write_read() {
	let s = new(MaxRecordSize::K1);

	let id = block_on(s.run(async {
		let obj = s.create().await.unwrap();
		obj.resize(1).await.unwrap();
		obj.write(0, &[1]).await.unwrap();
		Ok::<_, Error<_>>(obj.id())
	}))
	.unwrap();

	let s = block_on(async {
		let devs = s.unmount().await.unwrap();
		load(devs).await
	});

	block_on(s.run(async {
		let obj_1 = s.get(id).await.unwrap();
		let obj_2 = s.create().await.unwrap();
		obj_2.resize(1).await.unwrap();
		obj_2.write(0, &[2]).await.unwrap();

		let mut buf = [0];
		let l = obj_1.read(0, &mut buf).await.unwrap();
		assert_eq!(l, 1);
		assert_eq!(buf, [1]);

		let mut buf = [0];
		let l = obj_2.read(0, &mut buf).await.unwrap();
		assert_eq!(l, 1);
		assert_eq!(buf, [2]);
		Ok::<_, Error<_>>(())
	}))
	.unwrap();
}
