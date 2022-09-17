use crate::*;

#[derive(Debug)]
struct S(Box<[u8]>);

#[derive(Debug)]
enum Void {}

impl Storage for S {
	type Error = Void;

	fn block_count(&self) -> u64 {
		self.0.len() as u64 / 512
	}

	fn block_size_p2(&self) -> u8 {
		9
	}

	fn read(&mut self, lba: u64, blocks: usize) -> Result<Box<dyn nros::Read + '_>, Void> {
		struct R<'a>(&'a [u8]);
		impl Read for R<'_> {
			fn get(&self) -> &[u8] {
				self.0
			}
		}
		Ok(Box::new(R(&self.0[lba as usize * 512..][..blocks * 512])))
	}

	fn write(&mut self, lba: u64, blocks: usize) -> Result<Box<dyn nros::Write + '_>, Void> {
		assert!(
			(lba as usize + blocks) * 512 <= self.0.len(),
			"LBA + blocks out of range"
		);
		struct W<'a> {
			storage: &'a mut S,
			offset: usize,
			buf: Vec<u8>,
		}
		impl Write for W<'_> {
			fn get_mut(&mut self) -> &mut [u8] {
				&mut self.buf
			}

			fn set_blocks(&mut self, blocks: usize) {
				let len = blocks * 512;
				assert!(
					self.buf.capacity() >= len,
					"blocks out of range ({} >= {})",
					self.buf.capacity(),
					len
				);
				self.buf.resize(len, 0);
			}
		}
		impl Drop for W<'_> {
			fn drop(&mut self) {
				self.storage.0[self.offset..][..self.buf.len()].copy_from_slice(&self.buf);
			}
		}
		Ok(Box::new(W {
			storage: self,
			offset: lba as usize * 512,
			buf: vec![0; blocks * 512],
		}))
	}

	fn fence(&mut self) -> Result<(), Void> {
		Ok(())
	}
}

fn new() -> Nrfs<S> {
	let s = S(vec![0; 1 << 24].into());
	Nrfs::new(s, 10).unwrap()
}

#[test]
fn create_fs() {
	new();
}

#[test]
fn create_file() {
	let mut fs = new();
	let f = fs.create_file().unwrap();
	fs.storage.write_object(f, 0, b"Hello, world!").unwrap();
	dbg!();

	let mut d = fs.root_dir().unwrap();
	dbg!();
	d.insert(
		&mut fs,
		dir::NewEntry { data: dir::Data::Object(f), ty: dir::Type::File, name: b"test.txt" },
	)
	.unwrap();
	dbg!();

	assert!(d.find(&mut fs, b"I do not exist").unwrap().is_none());
	dbg!();
	let g = d.find(&mut fs, b"test.txt").unwrap().unwrap();
	dbg!();

	let mut buf = [0; 32];
	let l = g.read(&mut fs, 0, &mut buf).unwrap();
	assert_eq!(core::str::from_utf8(&buf[..l]), Ok("Hello, world!"));
}

#[test]
fn create_many_files() {
	let mut fs = new();

	// Create & read
	for i in 0..100 {
		let name = format!("{}.txt", i);
		let contents = format!("This is file #{}", i);

		let f = fs.create_file().unwrap();
		fs.storage.write_object(f, 0, contents.as_bytes()).unwrap();
		dbg!();

		let mut d = fs.root_dir().unwrap();
		dbg!();
		d.insert(
			&mut fs,
			dir::NewEntry {
				data: dir::Data::Object(f),
				ty: dir::Type::File,
				name: name.as_bytes(),
			},
		)
		.unwrap();
		dbg!();

		let g = d.find(&mut fs, name.as_bytes()).unwrap().unwrap();
		dbg!(f, &g);

		let mut buf = [0; 32];
		let l = g.read(&mut fs, 0, &mut buf).unwrap();
		assert_eq!(
			core::str::from_utf8(&buf[..l]),
			Ok(&*contents),
			"file #{}",
			i
		);
	}

	// Test iteration
	let d = fs.root_dir().unwrap();
	let mut i = 0;
	let mut count = 0;
	while let Some((e, ni)) = d.next_from(&mut fs, i).unwrap() {
		dbg!(e);
		count += 1;
		i = if let Some(i) = ni { i } else { break };
	}
	assert_eq!(count, 100);

	// Read only
	for i in 0..100 {
		let name = format!("{}.txt", i);
		let contents = format!("This is file #{}", i);

		let mut d = fs.root_dir().unwrap();
		dbg!();

		let g = d.find(&mut fs, name.as_bytes()).unwrap().unwrap();

		let mut buf = [0; 32];
		let l = g.read(&mut fs, 0, &mut buf).unwrap();
		assert_eq!(
			core::str::from_utf8(&buf[..l]),
			Ok(&*contents),
			"file #{}",
			i
		);
	}
}
