mod record;
mod record_tree;
mod write_buffer;

use crate::*;

fn new(max_record_size: MaxRecordSize) -> Nros<storage::MemoryDev> {
	let s = storage::MemoryDev::new(16, 10);
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

#[test]
fn write() {
	let mut s = new(MaxRecordSize::K1);
	let id = s.new_object().unwrap();
	s.resize(id, 2000).unwrap();
	s.write(id, 1000, &[0xcc; 1000]).unwrap();
}

#[test]
fn finish_transaction() {
	let mut s = new(MaxRecordSize::K1);
	let id = s.new_object().unwrap();
	s.resize(id, 2000).unwrap();
	s.write(id, 1000, &[0xcc; 1000]).unwrap();
	s.finish_transaction().unwrap();
}

#[test]
fn read_before_tx() {
	let mut s = new(MaxRecordSize::K1);
	let id = s.new_object().unwrap();
	s.resize(id, 2000).unwrap();
	s.write(id, 1000, &[0xcc; 1000]).unwrap();
	let mut buf = [0; 1000];
	s.read(id, 0, &mut buf).unwrap();
	assert_eq!(buf, [0; 1000]);
	s.read(id, 1000, &mut buf).unwrap();
	assert_eq!(buf, [0xcc; 1000]);
}

#[test]
fn read_after_tx() {
	let mut s = new(MaxRecordSize::K1);
	let id = s.new_object().unwrap();
	s.resize(id, 2000).unwrap();
	s.write(id, 1000, &[0xcc; 1000]).unwrap();
	s.finish_transaction().unwrap();
	let mut buf = [0; 1000];
	s.read(id, 0, &mut buf).unwrap();
	assert_eq!(buf, [0; 1000]);
	s.read(id, 1000, &mut buf).unwrap();
	assert_eq!(buf, [0xcc; 1000]);
}

#[test]
fn write_tx_read_many() {
	let mut s = new(MaxRecordSize::K1);

	let id = s.new_object().unwrap();
	s.resize(id, 2000).unwrap();
	s.write(id, 1000, &[0xcc; 1000]).unwrap();
	s.finish_transaction().unwrap();

	let id2 = s.new_object().unwrap();
	s.resize(id2, 64).unwrap();
	s.write(id2, 42, &[0xde; 2]).unwrap();
	s.finish_transaction().unwrap();

	let id3 = s.new_object().unwrap();
	s.resize(id3, 1).unwrap();
	s.write(id3, 0, &[1]).unwrap();
	s.finish_transaction().unwrap();

	let mut buf = [0; 1000];
	s.read(id, 0, &mut buf).unwrap();
	assert_eq!(buf, [0; 1000]);
	s.read(id, 1000, &mut buf).unwrap();
	assert_eq!(buf, [0xcc; 1000]);

	let mut buf = [0; 2];
	s.read(id2, 42, &mut buf).unwrap();
	assert_eq!(buf, [0xde; 2]);

	let mut buf = [0];
	s.read(id3, 0, &mut buf).unwrap();
	assert_eq!(buf, [1]);
}

#[test]
fn write_new_write() {
	let mut s = new(MaxRecordSize::K1);

	let id = s.new_object().unwrap();
	let id2 = s.new_object().unwrap();

	s.resize(id2, 64).unwrap();
	s.write(id2, 42, &[0xde; 2]).unwrap();

	s.resize(id, 2000).unwrap();
	s.write(id, 1000, &[0xcc; 1000]).unwrap();

	let mut buf = [0; 1000];
	s.read(id, 0, &mut buf).unwrap();
	assert_eq!(buf, [0; 1000]);
	s.read(id, 1000, &mut buf).unwrap();
	assert_eq!(buf, [0xcc; 1000]);

	s.move_object(id, id2).unwrap();

	let mut buf = [0; 2];
	s.read(id, 42, &mut buf).unwrap();
	assert_eq!(buf, [0xde; 2]);
}
