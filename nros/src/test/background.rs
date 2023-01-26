use super::*;

#[test]
fn stuck_0() {
	let s = new_cap(MaxRecordSize::K1, 1 << 16, 1177);

	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(111979766).await.unwrap();
	});
	block_on(bg.drop()).unwrap();

	let bg = Background::default();
	run2(&bg, async {
		let obj = s.get(&bg, 0).await.unwrap();
		let _len = obj.len().await.unwrap();
		obj.write(23362423067049983, &[1; 21313]).await.unwrap();
	});
	block_on(bg.drop()).unwrap();
}
