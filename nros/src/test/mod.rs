mod record_tree;

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

	fn read(&mut self, lba: u64, blocks: usize) -> Result<Box<dyn Read + '_>, Void> {
		struct R<'a>(&'a [u8]);
		impl Read for R<'_> {
			fn get(&self) -> &[u8] {
				self.0
			}
		}
		Ok(Box::new(R(&self.0[lba as usize * 512..][..blocks * 512])))
	}

	fn write(&mut self, lba: u64, blocks: usize) -> Result<Box<dyn Write + '_>, Void> {
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

fn new(max_record_size_p2: u8) -> Nros<S> {
	let s = S(vec![0; 1 << 24].into());
	Nros::new(s, max_record_size_p2).unwrap()
}

#[test]
fn resize_object() {
	let mut s = new(9);
	let id = s.new_object().unwrap();
	s.resize(id, 512).unwrap();
	s.resize(id, 1020).unwrap();
	s.resize(id, 500).unwrap();
	s.resize(id, 0).unwrap();
}
