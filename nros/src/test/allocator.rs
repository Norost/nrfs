use super::*;

/// Check if an object store is correctly saved before unmounting.
/// And also whether loading works.
#[test]
fn write_remount_read() {
	run(async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(1).await.unwrap();
		obj.write(0, &[1]).await.unwrap();

		let id = obj.id();
		drop(obj);
		let devs = s.unmount().await.unwrap();
		let s = Nros::load(devs, 1 << 12, 1 << 12).await.unwrap();

		let obj = s.get(id).await.unwrap();
		let mut buf = [0];
		let l = obj.read(0, &mut buf).await.unwrap();
		assert_eq!(l, 1);
		assert_eq!(buf, [1]);
	})
}

/// Check if an object store is correctly saved before unmounting.
/// And also whether loading works.
#[test]
fn write_remount_write_read() {
	run(async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(1).await.unwrap();
		obj.write(0, &[1]).await.unwrap();

		let id = obj.id();
		drop(obj);
		let devs = s.unmount().await.unwrap();
		let s = Nros::load(devs, 1 << 12, 1 << 12).await.unwrap();

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
	})
}
