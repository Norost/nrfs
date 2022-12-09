use {
	super::*,
	arbitrary::{Arbitrary, Unstructured},
	rangemap::RangeSet,
	rustc_hash::FxHashMap,
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
	contents: FxHashMap<u64, RangeSet<u64>>,
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
	/// Move an object.
	///
	/// This destroys the old object.
	Move { from_idx: u32, to_idx: u32 },
}

impl<'a> Arbitrary<'a> for Test {
	fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
		// Always start with a create.
		let create_op = Op::Create { size: u.arbitrary()? };
		Ok(Self::new(
			1 << 16, // Increase or decrease as you see fit.
			[Ok(create_op)]
				.into_iter()
				.chain(u.arbitrary_iter::<Op>()?)
				.try_collect::<Box<_>>()?,
		))
	}
}

impl Test {
	pub fn new(blocks: usize, ops: impl Into<Box<[Op]>>) -> Self {
		Self {
			store: run(|| new_cap(MaxRecordSize::K1, blocks, 4096, 4096)),
			ops: ops.into(),
			ids: Default::default(),
			contents: Default::default(),
		}
	}

	pub fn run(mut self) {
		run(|| async {
			for op in self.ops.into_vec() {
				match op {
					Op::Create { size } => {
						let obj = self.store.create().await.unwrap();
						obj.resize(size).await.unwrap();
						self.contents.insert(obj.id(), Default::default());
						self.ids.push(obj.id());
					}
					Op::Write { idx, offset, amount } => {
						let id = self.ids[idx as usize % self.ids.len()];
						let obj = self.store.get(id).await.unwrap();
						let len = obj.len().await.unwrap();
						if len > 0 {
							let offt = offset % len;
							let l = obj.write(offt, &vec![1; amount.into()]).await.unwrap();
							if l > 0 {
								self.contents
									.get_mut(&id)
									.unwrap()
									.insert(offt..offt + u64::try_from(l).unwrap());
							}
						}
					}
					Op::Read { idx, offset, amount } => {
						let id = self.ids[idx as usize % self.ids.len()];
						let obj = self.store.get(id).await.unwrap();
						let len = obj.len().await.unwrap();
						if len > 0 {
							let offt = offset % len;
							let buf = &mut vec![0; amount.into()];
							let l = obj.read(offt, buf).await.unwrap();
							if l > 0 {
								let map = self.contents.get(&id).unwrap();
								for (i, c) in (offt..offt + u64::try_from(l).unwrap()).zip(&*buf) {
									assert_eq!(map.contains(&i), *c == 1);
								}
							}
						}
					}
					Op::Remount => {
						let devs = self.store.unmount().await.unwrap();
						self.store = Nros::load(devs, 4096, 4096).await.unwrap();
					}
					Op::Move { from_idx, to_idx } => {
						let from_i = from_idx as usize % self.ids.len();
						let to_i = to_idx as usize % self.ids.len();
						let from_id = self.ids[from_i];
						let to_id = self.ids[to_i];
						if from_i != to_i {
							self.ids.swap_remove(from_i);
						}
						self.store
							.get(to_id)
							.await
							.unwrap()
							.replace_with(self.store.get(from_id).await.unwrap())
							.await
							.unwrap();
					}
				}
			}
		})
	}
}

use Op::*;

#[test]
fn unset_allocator_lba() {
	Test::new(512, [Create { size: 18446744073709486123 }, Remount]).run()
}

#[test]
fn allocator_save_space_leak() {
	Test::new(
		512,
		[
			Create { size: 18446744073709546299 },
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
		],
	)
	.run()
}

#[test]
fn large_object_shift_overflow() {
	Test::new(
		512,
		[
			Create { size: 18446567461959458655 },
			Write { idx: 4294967295, offset: 6917529024946200575, amount: 24415 },
		],
	)
	.run()
}

#[test]
fn tree_write_full_to_id_0() {
	Test::new(
		512,
		[
			Create { size: 18446587943058402107 },
			Remount,
			Create { size: 5425430176097894400 },
			Write { idx: 1263225675, offset: 21193410011155275, amount: 19275 },
		],
	)
	.run()
}

#[test]
fn tree_read_offset_len_check_overflow() {
	Test::new(
		1 << 16,
		[
			Create { size: 18446744073709551595 },
			Read { idx: 2509608341, offset: 18446744073709551509, amount: 38155 },
			Remount,
			Remount,
			Read { idx: 4287993237, offset: 697696064, amount: 0 },
		],
	)
	.run()
}

#[test]
fn cache_object_id_double_free_replace_with_self() {
	// Manually "reduced" a bit, mainly use zeroes everywhere
	Test::new(
		1 << 16,
		[
			Create { size: 0 },
			Move { from_idx: 0, to_idx: 0 },
			Move { from_idx: 0, to_idx: 0 },
		],
	)
	.run()
}

#[test]
fn tree_shrink_unimplemented() {
	// Ditto
	Test::new(
		1 << 16,
		[
			Create { size: 6872316419617283935 },
			Write { idx: 0, offset: 18446744073709551455, amount: 65476 },
			Create { size: 18446744073709486080 },
			Move { from_idx: 1, to_idx: 0 },
		],
	)
	.run()
}

#[test]
fn cache_move_object_stale_lru() {
	Test::new(
		1 << 16,
		[
			Create { size: 18446721160059038699 },
			Create { size: 18442240474082180864 },
			Move { from_idx: 1600085852, to_idx: 1600085855 },
			Write { idx: 1600085855, offset: 6872316419617283935, amount: 24415 },
		],
	)
	.run()
}

#[test]
fn cache_get_large_shift_offset() {
	Test::new(
		1 << 16,
		[
			Create { size: 6872316419617283935 },
			Write { idx: 4294926175, offset: 18446744073709551615, amount: 24575 },
			Write { idx: 1600085855, offset: 71777215877963615, amount: 17247 },
			Create { size: 18446743382226067295 },
			Move { from_idx: 255, to_idx: 0 },
		],
	)
	.run()
}
