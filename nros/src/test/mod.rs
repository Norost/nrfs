mod record;
mod record_tree;

use crate::*;

fn new(max_record_size: MaxRecordSize) -> Nros<storage::MemoryDev> {
	let s = storage::MemoryDev::new(8, 10);
	Nros::new(s, max_record_size, Compression::None, 2).unwrap()
}

#[test]
fn resize_object() {
	let mut s = new(MaxRecordSize::K1);
	let id = s.new_object().unwrap();
	s.resize(id, 1024).unwrap();
	s.resize(id, 2040).unwrap();
	s.resize(id, 1000).unwrap();
	s.resize(id, 0).unwrap();
}
