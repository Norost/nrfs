use super::*;

/// Check if an object store is correctly saved before unmounting.
/// And also whether loading works.
#[test]
fn write_remount_read() {
	run(|| async {
		let s = new(MaxRecordSize::K1).await;
		let obj = s.create().await.unwrap();
		obj.resize(1).await.unwrap();
		obj.write(1, &[1]).await.unwrap();

		drop(obj);
		let devs = s.unmount().await.unwrap();

		let s = Nros::load(devs, 1 << 12, 1 << 12).await.unwrap();
	})
}
