use super::*;

/// Check if an object store is correctly saved before unmounting.
/// And also whether loading works.
#[test]
fn write_remount_read() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	let id = block_on(async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1).await.unwrap();
		obj.write(0, &[1]).await.unwrap();

		obj.id()
	});
	block_on(bg.drop()).unwrap();

	let s = block_on(async {
		let devs = s.unmount().await.unwrap();
		Nros::load(StdResource::new(), devs, 1 << 12, true)
			.await
			.unwrap()
	});

	let bg = Background::default();
	run2(&bg, async {
		let obj = s.get(&bg, id).await.unwrap();
		let mut buf = [0];
		let l = obj.read(0, &mut buf).await.unwrap();
		assert_eq!(l, 1);
		assert_eq!(buf, [1]);
	});
	block_on(bg.drop()).unwrap();
}

/// Check if an object store is correctly saved before unmounting.
/// And also whether loading works.
#[test]
fn write_remount_write_read() {
	let s = new(MaxRecordSize::K1);
	let bg = Background::default();
	let id = block_on(async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1).await.unwrap();
		obj.write(0, &[1]).await.unwrap();

		obj.id()
	});
	block_on(bg.drop()).unwrap();

	let s = block_on(async {
		let devs = s.unmount().await.unwrap();
		Nros::load(StdResource::new(), devs, 1 << 12, true)
			.await
			.unwrap()
	});

	let bg = Background::default();
	run2(&bg, async {
		let obj_1 = s.get(&bg, id).await.unwrap();
		let obj_2 = s.create(&bg).await.unwrap();
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
	});
	block_on(bg.drop()).unwrap();
}
