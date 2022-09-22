use super::*;

fn new() -> RecordCache<storage::MemoryDev> {
	let s = storage::MemoryDev::new(16, 10);
	RecordCache::new(s, MaxRecordSize::K1, Compression::None)
}

#[test]
fn grow_0() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 1024).unwrap();
}

#[test]
fn grow_1() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 1025).unwrap();
}

#[test]
fn shrink_0_to_0() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 1024).unwrap();
	t.resize(&mut c, 100).unwrap();
}

#[test]
fn shrink_1_to_0() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 1025).unwrap();
	t.resize(&mut c, 1024).unwrap();
}

#[test]
fn write_0() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 1024).unwrap();
	t.write(&mut c, 0, &[0xcc; 1024]).unwrap();
}

#[test]
fn write_0_offset() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 1024).unwrap();
	t.write(&mut c, 512, &[0xcc; 512]).unwrap();
}

#[test]
fn write_0_twice() {
	let mut c = new();
	let mut t = RecordTree::default();
	t.resize(&mut c, 1024).unwrap();
	t.write(&mut c, 0, &[0xcc; 1024]).unwrap();
	t.write(&mut c, 0, &[0xcc; 1024]).unwrap();
}
