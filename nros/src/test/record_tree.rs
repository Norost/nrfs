use super::*;

fn new() -> RecordCache<S> {
	let s = S(vec![0; 1 << 16].into());
	RecordCache::new(s, 9)
}

#[test]
fn grow_0() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 512).unwrap();
}

#[test]
fn grow_1() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 513).unwrap();
}

#[test]
fn shrink_0_to_0() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 512).unwrap();
	t.resize(&mut c, 500).unwrap();
}

#[test]
fn shrink_1_to_0() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 513).unwrap();
	t.resize(&mut c, 512).unwrap();
}

#[test]
fn write_0() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 512).unwrap();
	t.write(&mut c, 0, &[0xcc; 512]).unwrap();
}

#[test]
fn write_0_offset() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 512).unwrap();
	t.write(&mut c, 256, &[0xcc; 256]).unwrap();
}

#[test]
fn write_0_twice() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 512).unwrap();
	t.write(&mut c, 0, &[0xcc; 512]).unwrap();
	t.write(&mut c, 0, &[0xcc; 512]).unwrap();
}
