/// Test added after the hard memory limit was introduced.
use super::*;

#[test]
pub fn deadlock_0() {
	let s = new_cap(MaxRecordSize::K1, 1 << 16, 0);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.resize(0x100000000000000).await.unwrap();
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
		obj.resize(0xffffffffff04003a).await.unwrap();
		obj.write(0xcb01ffffffffffff, &[1; 0xffff]).await.unwrap();
		obj.decrease_reference_count().await.unwrap();
		Ok(())
	});
	block_on(s.unmount()).unwrap();
}

/// `EntryRef::write_zeros` used an improper bounds check.
#[test]
pub fn entry_write_zeros_pseudo_object_leak() {
	let s = new_cap(MaxRecordSize::K1, 1 << 16, 511);
	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.resize(0x1000000000000000).await.unwrap();
		obj.write(0x10000, &[1; 0x4000]).await.unwrap();
		obj.resize(0x20000).await.unwrap();
		Ok(())
	});
	block_on(s.unmount()).unwrap();
}

/// We call `memory_reserve_object` but didn't refetch the object, let alone the root.
#[test]
pub fn tree_shrink_missing_object() {
	let s = new_cap(MaxRecordSize::K1, 1 << 16, 0);
	run(&s, async {
		let obj_a = s.create().await.unwrap();
		obj_a.resize(0x76130003).await.unwrap();
		let obj_b = s.create().await.unwrap();
		obj_b.resize(0xffff00fd0d2d0d).await.unwrap();
		obj_b.write(0xd1a003362b656, &[1; 0xd0d]).await.unwrap();
		obj_a.decrease_reference_count().await.unwrap();
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
		obj_c.resize(0xffffffff1b1b1b1b).await.unwrap();
		let obj_d = s.create().await.unwrap();
		obj_d.resize(0x1).await.unwrap();

		obj_d.decrease_reference_count().await.unwrap();

		obj_c.write(0x2dabababab2dabab, &[1; 0xabab]).await.unwrap();

		Ok(())
	});
	block_on(s.unmount()).unwrap();
}

/// We put an await point (with `memory_reserve`) even though the steps told us not to.
#[test]
pub fn tree_grow_move_root_race() {
	let s = new_cap(MaxRecordSize::K1, 1 << 16, 194);
	run(&s, async {
		let obj_a = s.create().await.unwrap();
		obj_a.resize(0x69e62700c200018b).await.unwrap();
		obj_a.write(0x4145042a692b29a0, &[1; 0x2b2b]).await.unwrap();

		let obj_b = s.create().await.unwrap();
		obj_b.resize(0x2b2b0000ffff0000).await.unwrap();
		obj_b.write(0x1b1b1b1b1b1b2525, &[1; 0x1b1b]).await.unwrap();

		let _ = s.create().await.unwrap();

		let obj_d = s.create().await.unwrap();
		obj_d.resize(0x1b1b1b1b1b1b1b1b).await.unwrap();

		for i in 0..2 {
			let obj = s.create().await.unwrap();
		}

		obj_d.decrease_reference_count().await.unwrap();

		for _ in 0..12 {
			let _ = s.create().await.unwrap();
		}

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

#[test]
fn grow_write_shrink_many() {
	let s = new(MaxRecordSize::K1);
	run(&s, async {
		let obj = s.create().await.unwrap();

		for _ in 0..200 {
			obj.resize(1).await.unwrap();
			obj.write(0, &[1]).await.unwrap();
			obj.resize(0).await.unwrap();
		}

		Ok(())
	});
}
