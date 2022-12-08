use {
	super::*,
	arbitrary::{Arbitrary, Unstructured},
};

#[derive(Debug)]
pub struct Test {
	/// Object store to operate on.
	store: Nros<MemDev>,
	/// Ops to execute
	ops: Box<[Op]>,
	/// Valid (created) objects.
	ids: Vec<u64>,
	/// Expected contents of each object.
	///
	/// We only write `1`s, so it's pretty simple.
	contents: (),
}

#[derive(Debug, Arbitrary)]
pub enum Op {
	/// Create an object.
	Create { size: u64 },
	/// Write to an object.
	///
	/// `idx` is and index in `ids`.
	/// `offset` is modulo size of object.
	Write { idx: u32, offset: u64, amount: u16 },
	/// Read from an object.
	///
	/// `idx` is and index in `ids`.
	/// `offset` is modulo size of object.
	///
	/// Also verifies contents.
	Read { idx: u32, offset: u64, amount: u16 },
	/// Remount the filesystem.
	Remount,
}

impl<'a> Arbitrary<'a> for Test {
	fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
		// Always start with a create.
		let create_op = Op::Create { size: u.arbitrary()? };
		Ok(Self::new(
			[Ok(create_op)]
				.into_iter()
				.chain(u.arbitrary_iter::<Op>()?)
				.try_collect::<Box<_>>()?,
		))
	}
}

impl Test {
	pub fn new(ops: impl Into<Box<[Op]>>) -> Self {
		Self {
			store: run(|| new_cap(MaxRecordSize::K1, 512, 4096, 4096)),
			ops: ops.into(),
			ids: Default::default(),
			contents: (),
		}
	}

	pub fn run(mut self) {
		run(|| async {
			for op in self.ops.into_vec() {
				match op {
					Op::Create { size } => {
						let obj = self.store.create().await.unwrap();
						obj.resize(size).await.unwrap();
						self.ids.push(obj.id());
					}
					Op::Write { idx, offset, amount } => {
						let id = self.ids[idx as usize % self.ids.len()];
						let obj = self.store.get(id).await.unwrap();
						let len = obj.len().await.unwrap();
						if len > 0 {
							obj.write(offset % len, &vec![1; amount.into()])
								.await
								.unwrap();
						}
					}
					Op::Read { idx, offset, amount } => {
						let id = self.ids[idx as usize % self.ids.len()];
						let obj = self.store.get(id).await.unwrap();
						let len = obj.len().await.unwrap();
						if len > 0 {
							obj.read(offset % len, &mut vec![0; amount.into()])
								.await
								.unwrap();
						}
					}
					Op::Remount => {
						let devs = self.store.unmount().await.unwrap();
						self.store = Nros::load(devs, 4096, 4096).await.unwrap();
					}
				}
			}
		})
	}
}

use Op::*;

#[test]
fn unset_allocator_lba() {
	Test::new([Create { size: 18446744073709486123 }, Remount]).run()
}

#[test]
fn allocator_save_space_leak() {
	Test::new([
		Create {
			size: 18446744073709546299,
		},
		Remount,
		Remount,
		Remount,
		Remount,
		Remount,
		Remount,
		Remount,
		Remount,
		Remount,
		Remount,
		Remount,
		Remount,
		Remount,
		Remount,
	]).run()
}

#[test]
fn large_object_shift_overflow() {
	Test::new([
		Create {
			size: 18446567461959458655,
		},
		Write {
			idx: 4294967295,
			offset: 6917529024946200575,
			amount: 24415,
		},
	]).run()
}
