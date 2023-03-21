/// Test added after the hard memory limit was introduced.
use super::*;

#[test]
pub fn deadlock_0() {
	let s = new_cap(MaxRecordSize::K1, 1 << 16, 0);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(0xc7ffffffff2800, &[1; 0x6000]).await.unwrap();
		Ok(())
	});
	block_on(s.unmount()).unwrap();
}

#[test]
pub fn missing_entry() {
	let s = new_cap(MaxRecordSize::K1, 1 << 16, 511);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(0xcb01ffffffffffff, &[1; 0xffff]).await.unwrap();
		obj.dealloc().await.unwrap();
		Ok(())
	});
	block_on(s.unmount()).unwrap();
}

/// In [`Tree::get`] there was a race condition
#[test]
pub fn tree_get_wait_entry_race() {
	let s = new_cap(MaxRecordSize::K1, 1 << 16, 0);
	run(&s, async {
		let _obj_a = s.create().await.unwrap();
		let _obj_b = s.create().await.unwrap();
		let obj_c = s.create().await.unwrap();
		let obj_d = s.create().await.unwrap();

		obj_d.dealloc().await.unwrap();

		obj_c.write(0x2dabababab2dabab, &[1; 0xabab]).await.unwrap();

		Ok(())
	});
	block_on(s.unmount()).unwrap();
}

#[test]
fn create_many() {
	let s = new_cap(MaxRecordSize::K1, 1 << 12, 0);
	run(&s, async {
		for _ in 0..512 {
			s.create().await.unwrap();
		}
		Ok(())
	});
	block_on(s.unmount()).unwrap();
}
