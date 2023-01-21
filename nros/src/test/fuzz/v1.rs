use {
	super::*,
	arbitrary::{Arbitrary, Unstructured},
	rangemap::RangeSet,
	rustc_hash::FxHashMap,
};

#[derive(Debug)]
pub struct Test {
	/// Object store to operate on.
	store: Nros<MemDev, StdResource>,
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
	/// Resize an object.
	Resize { idx: u32, size: u64 },
}

impl<'a> Arbitrary<'a> for Test {
	fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
		// Always start with a create.
		let create_op = Op::Create { size: u.arbitrary()? };

		let dirty_cache_size = u.int_in_range(1024..=1 << 24)?;
		let global_cache_size = u.int_in_range(dirty_cache_size..=1 << 24)?;

		Ok(Self::new(
			1 << 16,
			global_cache_size,
			[Ok(create_op)]
				.into_iter()
				.chain(u.arbitrary_iter::<Op>()?)
				/*
				.map(|op| {
					op.map(|op| match op {
						Op::Create { size } => Op::Create { size: size % m },
						Op::Write { idx, offset, amount } => {
							Op::Write { idx, offset: offset % m, amount }
						}
						Op::Read { idx, offset, amount } => {
							Op::Read { idx, offset: offset % m, amount }
						}
						Op::Resize { idx, size } => Op::Resize { idx, size: size % m },
						op => op,
					})
				})
				*/
				.try_collect::<Box<_>>()?,
		))
	}
}

impl Test {
	pub fn new(blocks: usize, global_cache: usize, ops: impl Into<Box<[Op]>>) -> Self {
		Self {
			store: new_cap(MaxRecordSize::K1, blocks, global_cache),
			ops: ops.into(),
			ids: Default::default(),
			contents: Default::default(),
		}
	}

	pub fn run(mut self) {
		for op in self.ops.into_vec() {
			match op {
				Op::Create { size } => {
					let bg = Background::default();
					run2(&bg, async {
						let obj = self.store.create(&bg).await.unwrap();
						obj.resize(size).await.unwrap();
						self.contents.insert(obj.id(), Default::default());
						self.ids.push(obj.id());
					});
					block_on(bg.drop()).unwrap();
				}
				Op::Write { idx, offset, amount } => {
					let bg = Background::default();
					run2(&bg, async {
						let id = self.ids[idx as usize % self.ids.len()];
						let obj = self.store.get(&bg, id).await.unwrap();
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
					});
					block_on(bg.drop()).unwrap();
				}
				Op::Read { idx, offset, amount } => {
					let bg = Background::default();
					run2(&bg, async {
						let id = self.ids[idx as usize % self.ids.len()];
						let obj = self.store.get(&bg, id).await.unwrap();
						let len = obj.len().await.unwrap();
						if len > 0 {
							let offt = offset % len;
							let buf = &mut vec![0; amount.into()];
							let l = obj.read(offt, buf).await.unwrap();
							if l > 0 {
								let map = self.contents.get(&id).unwrap();
								for (i, c) in (offt..offt + u64::try_from(l).unwrap()).zip(&*buf) {
									let expect = u8::from(map.contains(&i));
									assert!(
										expect == *c,
										"expected {}, got {} (offset: {})",
										expect,
										*c,
										i
									);
								}
							}
						}
					});
					block_on(bg.drop()).unwrap();
				}
				Op::Remount => {
					self.store = block_on(async {
						let devs = self.store.unmount().await.unwrap();
						Nros::load(StdResource::new(), devs, 4096, true)
							.await
							.unwrap()
					})
				}
				Op::Move { from_idx, to_idx } => {
					let bg = Background::default();
					run2(&bg, async {
						let from_i = from_idx as usize % self.ids.len();
						let to_i = to_idx as usize % self.ids.len();
						let from_id = self.ids[from_i];
						let to_id = self.ids[to_i];
						if from_i != to_i {
							self.ids.swap_remove(from_i);
						}
						self.store
							.get(&bg, to_id)
							.await
							.unwrap()
							.replace_with(self.store.get(&bg, from_id).await.unwrap())
							.await
							.unwrap();

						let c = self.contents.remove(&from_id).unwrap();
						self.contents.insert(to_id, c);
					});
					block_on(bg.drop()).unwrap();
				}
				Op::Resize { idx, size } => {
					let bg = Background::default();
					run2(&bg, async {
						let id = self.ids[idx as usize % self.ids.len()];
						let obj = self.store.get(&bg, id).await.unwrap();
						obj.resize(size).await.unwrap();
						if size < u64::MAX {
							self.contents.get_mut(&id).unwrap().remove(size..u64::MAX);
						}
					});
					block_on(bg.drop()).unwrap();
				}
			}
		}
	}
}

use Op::*;

#[test]
fn unset_allocator_lba() {
	Test::new(512, 4096, [Create { size: 18446744073709486123 }, Remount]).run()
}

#[test]
fn allocator_save_space_leak() {
	Test::new(
		512,
		4096,
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
		4096,
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
		4096,
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
		4096,
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
		4096,
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
		4096,
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
		4096,
		[
			Create { size: 18446721160059038699 },
			Create { size: 18442240474082180864 },
			Move { from_idx: 0, to_idx: 1 },
			Write { idx: 1, offset: 6872316419617283935, amount: 24415 },
		],
	)
	.run()
}

#[test]
fn cache_get_large_shift_offset() {
	Test::new(
		1 << 16,
		4096,
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

#[test]
fn tree_shrink_divmod_record_size() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 40002 },
			Resize { idx: 0, size: 40001 },
			Write { idx: 0, offset: 0, amount: 12994 },
		],
	)
	.run()
}

#[test]
fn tree_grow_add_record_write_cache_size() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 217080124979886539 },
			Write { idx: 960051513, offset: 18446742978491529529, amount: 65535 },
			Resize { idx: 4293656575, size: 18301847378652561407 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn tree_get_target_depth_above_dev_depth() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 281474976655851 },
			Write { idx: 4293603329, offset: 18446743984320625663, amount: 65535 },
			Resize { idx: 4294967295, size: 16947046754988065023 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn tree_grow_flush_concurrent() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 217080124979923771 },
			Write { idx: 960051513, offset: 4123390705508810553, amount: 65535 },
			Resize { idx: 4294967295, size: 1889604303433841151 },
		],
	)
	.run()
}

#[test]
fn grow_root_double_ref() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 1 << 60 },
			Write { idx: 0, offset: 96641949647915046, amount: u16::MAX },
			Resize { idx: 0, size: u64::MAX },
			Remount,
		],
	)
	.run()
}

#[test]
fn tree_shrink_destroy_depth_off_by_one() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 6223798073269682271 },
			Write { idx: 0, offset: 5999147927136639863, amount: 65286 },
			Remount,
			Resize { idx: 0, size: 1 },
		],
	)
	.run()
}

/// This case made me realize [`Tree::shrink`] was overly complex.
///
/// Now it is still complex but at least it works now,
/// at least until I run the fuzzer again.
#[test]
fn tree_write_shrink_from_scratch_0() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 6223798073269682271 },
			Write { idx: 0, offset: 5999147927136639863, amount: 65286 },
			Resize { idx: 0, size: 432345568522491477 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn tree_shrink_shift_overflow() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 17870283318554001399 },
			Resize { idx: 0, size: 17868031521458223095 },
		],
	)
	.run()
}

#[test]
fn write_resize_double_free() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 18446744073692785643 },
			Write { idx: 3828083684, offset: 16441494229869395940, amount: 11236 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn tree_resize_another_use_after_free() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 3544668469065756931 },
			Write { idx: 0, offset: 3544668469065756977, amount: 38807 },
			Resize { idx: 0, size: 1 },
		],
	)
	.run()
}

#[test]
fn tree_write_shrink() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 6148820866244280425 },
			Write { idx: 0, offset: 18398705676741115903, amount: 65535 },
			Resize { idx: 0, size: 1 },
		],
	)
	.run()
}

#[test]
fn tree_write_resize_0_double_free() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 18390793239351867851 },
			Write { idx: 0, offset: 18373873072982296662, amount: 65279 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn tree_write_resize_1_double_free() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 18390793239351867851 },
			Write { idx: 0, offset: 18373873072982296662, amount: 65279 },
			Resize { idx: 0, size: 1 },
		],
	)
	.run()
}

#[test]
fn tree_write_shrink_shrink_use_after_free() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 18446462598732840961 },
			Write { idx: 0, offset: 18390793471280101717, amount: 65535 },
			Resize { idx: 0, size: 18388299555398483947 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn tree_shrink_idk_man() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 18446744073709551614 },
			Write { idx: 65501, offset: 15987206784517266935, amount: 56797 },
			Resize { idx: 7415, size: 15987205831607189504 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn tree_reeeeeeeeeeeeee() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 827 },
			Resize { idx: 0, size: 16999953897322704879 },
			Write { idx: 0, offset: 2047, amount: 65535 },
			Resize { idx: 0, size: 1099511628031 },
		],
	)
	.run()
}

/// It was a bug in the test runner itself, amazing...
#[test]
fn test_small_resize() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 1003 },
			Write { idx: 0, offset: 0, amount: 2 },
			Resize { idx: 0, size: 1 },
			Resize { idx: 0, size: 2 },
			Read { idx: 0, offset: 0, amount: 2 },
		],
	)
	.run()
}

#[test]
fn unflushed_empty_dirty_entries() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 1026 },
			Write { idx: 0, offset: 1025, amount: 1 },
			Remount,
			Resize { idx: 0, size: 1025 },
			Remount,
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn create_shrink() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 1 << 21 },
			Resize { idx: 0, size: (1 << 20) + 1 },
			Remount,
		],
	)
	.run()
}

#[test]
fn god_have_mercy_upon_me() {
	Test::new(
		1 << 16,
		1 << 24,
		[
			Create { size: 18446494612532378059 },
			Create { size: 96077500568653133 },
			Write { idx: 1, offset: 18446462784140684075, amount: 65535 },
			Move { from_idx: 1, to_idx: 0 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn move_object_not_present() {
	Test::new(
		1 << 16,
		1 << 24,
		[
			Create { size: 0 },
			Create { size: 0 },
			Move { from_idx: 1, to_idx: 0 },
		],
	)
	.run()
}

#[test]
fn grow_shrink_unflushed_dirty() {
	Test::new(
		1 << 16,
		1024,
		[
			Create { size: 6356832 },
			Resize { idx: 0, size: 18446587333172986083 },
			Resize { idx: 0, size: 16740721 },
		],
	)
	.run()
}

#[test]
fn grow_set_stale_root() {
	Test::new(
		1 << 16,
		1 << 24,
		[
			Create { size: u64::MAX },
			Write { idx: 0, offset: 0x4747474747474747, amount: 0x4747 },
			Write { idx: 0, offset: 0x4747474747474747, amount: 0x4747 },
			Create { size: 65 },
		],
	)
	.run()
}

/// This was previously called `grow_redundant_add_record`.
/// However, contrary to my previous belief, the record added is *not* redundant.
///
/// Consider the following tree, where `o` is in cache and `.` is on-disk:
///
/// ```
///     __.__
///    /     \
///   .       .
///  / \     / \
/// .   .   .   .
/// ```
///
/// Suppose the rightmost leaf gets modified:
///
/// ```
///     __.__
///    /     \
///   .       o
///  / \     / \
/// .   .   .   o
/// ```
///
/// Suppose the tree is grown and a level is added:
///
/// ```
///       .
///       |
///     __.__
///    /     \
///   .       o
///  / \     / \
/// .   .   .   o
/// ```
///
/// Suppose an attempt to read the leftmost leaf begins:
///
/// ```
///       o
///       |
///     __.__
///    /     \
///   .       .
///  / \     / \
/// .   .   .   o
/// ```
///
/// The leftmost leaf will be considered zero since the root is zero!
#[test]
fn grow_add_record_race() {
	Test::new(
		1 << 16,
		1 << 20,
		[
			Create { size: 919 },
			Write { idx: 0, offset: 917, amount: 1 },
			Resize { idx: 0, size: 1025 },
		],
	)
	.run()
}

#[test]
fn move_object_stale_root() {
	Test::new(
		1 << 16,
		2556,
		[
			Create { size: 514373878767853 },
			Create { size: 2965947086361143593 },
			Write { idx: 1, offset: 12804209971436716032, amount: 45443 },
			Move { from_idx: 1, to_idx: 0 },
		],
	)
	.run();
}

#[test]
fn resize_in_range_zeroed() {
	Test::new(
		1 << 16,
		1 << 24,
		[
			Create { size: 1025 },
			Write { idx: 0, offset: 0, amount: 1 },
			Resize { idx: 0, size: 1 },
			Read { idx: 0, offset: 0, amount: 1 },
		],
	)
	.run()
}

/// This test detected the wrong assumption made in `grow_add_record_race`,
/// previously called `grow_redundant_add_record`.
#[test]
fn write_unmount_write_grow_data_loss() {
	Test::new(
		1 << 16,
		1 << 24,
		[
			Create { size: 288230403347578881 },
			Write { idx: 0, offset: 0, amount: 1 },
			Remount,
			Write { idx: 0, offset: 261207326956930858, amount: 14144 },
			Resize { idx: 0, size: 10376293537170274103 },
			Read { idx: 0, offset: 0, amount: 1 },
		],
	)
	.run()
}

#[test]
fn stuck_0() {
	Test::new(
		1 << 16,
		1177,
		[
			Create { size: 111979766 },
			Write { idx: 0, offset: 53726419, amount: 21313 },
		],
	)
	.run()
}

#[test]
fn shrink_unflushed_0() {
	Test::new(
		1 << 16,
		1 << 24,
		[
			Create { size: 16 },
			Resize { idx: 0, size: 1 << 20 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn shrink_unflushed_1() {
	Test::new(
		1 << 16,
		1439,
		[
			Create { size: 281470681743393 },
			Write { idx: 0, offset: 7700874272879, amount: u16::MAX },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

/// Entry evictions running in the background can interfere with [`Tree::shrink`].
#[test]
fn shrink_background_evict() {
	Test::new(
		1 << 16,
		4096,
		[
			Create { size: 6223798073269682271 },
			Write { idx: 0, offset: 5999147927136639863, amount: 65286 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn shrink_use_after_free() {
	Test::new(
		1 << 16,
		2045,
		[
			Create { size: 27075473812832 },
			Write { idx: 0, offset: 16919377365408, amount: 65535 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn flush_entry_not_present() {
	Test::new(
		1 << 16,
		58160,
		[
			Create { size: 2377900809628855616 },
			Write { idx: 4294926591, offset: 18446742974197925947, amount: 65535 },
			Remount,
		],
	)
	.run()
}

#[test]
fn shrink_wrong_transfer_offset() {
	Test::new(
		1 << 16,
		1842,
		[
			Create { size: 1026 },
			Resize { idx: 0, size: 1025 },
			Resize { idx: 0, size: 1 },
		],
	)
	.run()
}

#[test]
fn move_object_fix_lru_object() {
	Test::new(
		1 << 16,
		1311,
		[
			Create { size: 0 },
			Create { size: 1 << 20 },
			Move { from_idx: 0, to_idx: 1 },
		],
	)
	.run()
}

#[test]
fn flush_all_object_roots_background_barrier() {
	Test::new(
		1 << 16,
		1024,
		[
			Create { size: 62 },
			Create { size: 11469823825814749087 },
			Resize { idx: 1, size: 6989585522167382016 },
			Remount,
		],
	)
	.run()
}

#[test]
fn object_root_evict_fetch_race() {
	Test::new(
		1 << 16,
		1183,
		[
			Create { size: 0 },
			Create { size: 2951456134979886889 },
			Move { from_idx: 1, to_idx: 0 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn unref_non_zero_pseudo_object() {
	Test::new(
		1 << 16,
		1 << 20,
		[
			Create { size: 18446744073709486176 },
			Write { idx: 2667595591, offset: 17892238369727643649, amount: 20220 },
			Remount,
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}
