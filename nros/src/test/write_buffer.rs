use super::*;

fn new() -> RecordCache<storage::MemoryDev> {
	let s = storage::MemoryDev::new(16, 10);
	RecordCache::new(s, MaxRecordSize::K1, Compression::None, 4)
}

#[test]
fn write_0() {
	let mut c = new();
	let mut t = RecordTree::default();
	let mut w = WriteBuffer::new(&t);
	w.resize(1024);
	w.write(&mut c, &t, 0, &[0xcc; 1024]).unwrap();
	w.flush(&mut c, &mut t).unwrap();
	let mut b = [0; 1024];
	w.read(&mut c, &t, 0, &mut b).unwrap();
	assert_eq!(&b, &[0xcc; 1024]);
}

#[test]
fn write_0_offset() {
	let mut c = new();
	let mut t = RecordTree::default();
	let mut w = WriteBuffer::new(&t);
	w.resize(1024);
	w.write(&mut c, &t, 512, &[0xcc; 512]).unwrap();
	w.flush(&mut c, &mut t).unwrap();
	let mut b = [0; 1024];
	w.read(&mut c, &t, 0, &mut b).unwrap();
	assert_eq!(&b[..512], &[0; 512]);
	assert_eq!(&b[512..], &[0xcc; 512]);
}

#[test]
fn write_0_twice() {
	let mut c = new();
	let mut t = RecordTree::default();
	let mut w = WriteBuffer::new(&t);
	w.resize(1024);
	w.write(&mut c, &t, 0, &[0xcc; 1024]).unwrap();
	w.write(&mut c, &t, 0, &[0xdd; 1024]).unwrap();
	w.flush(&mut c, &mut t).unwrap();
	let mut b = [0; 1024];
	w.read(&mut c, &t, 0, &mut b).unwrap();
	assert_eq!(&b, &[0xdd; 1024]);
}

#[test]
fn write_0_append() {
	let mut c = new();
	let mut t = RecordTree::default();
	let mut w = WriteBuffer::new(&t);
	w.resize(1024);
	w.write(&mut c, &t, 0, &[0xcc; 512]).unwrap();
	w.write(&mut c, &t, 512, &[0xdd; 512]).unwrap();
	w.flush(&mut c, &mut t).unwrap();
	let mut b = [0; 1024];
	w.read(&mut c, &t, 0, &mut b).unwrap();
	assert_eq!(&b[..512], &[0xcc; 512]);
	assert_eq!(&b[512..], &[0xdd; 512]);
}

/// Because dumbass me blindly strips off zeroes from data :)
#[test]
fn write_0_append_clear() {
	let mut c = new();
	let mut t = RecordTree::default();
	let mut w = WriteBuffer::new(&t);
	w.resize(1024);
	w.write(&mut c, &t, 0, &[0xcc; 512]).unwrap();
	w.write(&mut c, &t, 512, &[0xdd; 512]).unwrap();
	w.write(&mut c, &t, 0, &[0; 512]).unwrap();
	w.flush(&mut c, &mut t).unwrap();
	let mut b = [0; 1024];
	w.read(&mut c, &t, 0, &mut b).unwrap();
	assert_eq!(&b[..512], &[0; 512]);
	assert_eq!(&b[512..], &[0xdd; 512]);
}

#[test]
fn write_0_append_clear2() {
	let mut c = new();
	let mut t = RecordTree::default();
	let mut w = WriteBuffer::new(&t);
	w.resize(512);
	w.write(&mut c, &t, 256, &[0xdd; 256]).unwrap();
	w.write(&mut c, &t, 0, &[0xcc; 256]).unwrap();
	w.write(&mut c, &t, 256, &[0; 255]).unwrap();
	w.flush(&mut c, &mut t).unwrap();
	let mut b = [0; 512];
	w.read(&mut c, &t, 0, &mut b).unwrap();
	assert_eq!(&b[..256], &[0xcc; 256]);
	assert_eq!(&b[256..511], &[0; 255]);
	assert_eq!(&b[511..], &[0xdd; 1]);
}
