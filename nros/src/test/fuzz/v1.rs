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
	ops: Vec<Op>,
	/// Valid (created) objects.
	ids: Vec<u64>,
	/// Expected contents of each object.
	///
	/// We only write `1`s, so it's pretty simple.
	contents: FxHashMap<u64, RangeSet<u64>>,
}

#[derive(Arbitrary)]
pub enum Op {
	/// Create an object.
	Create { size: u64 },
	/// Write to an object.
	///
	/// `idx` is and index in `ids`.
	/// `offset` is modulo size of object.
	Write { idx: u8, offset: u64, amount: u16 },
	/// Read from an object.
	///
	/// `idx` is and index in `ids`.
	/// `offset` is modulo size of object.
	///
	/// Also verifies contents.
	Read { idx: u8, offset: u64, amount: u16 },
	/// Remount the filesystem.
	Remount { cache_size: [u8; 3] },
	/// Move an object.
	///
	/// This destroys the old object.
	Move { from_idx: u8, to_idx: u8 },
	/// Resize an object.
	Resize { idx: u8, size: u64 },
	/// Destroy an object.
	///
	/// This decrements the reference count once, then forgets the object.
	///
	/// If no objects remain, [`Test::run`] stops and unmounts the filesystem.
	Destroy { idx: u8 },
}

impl fmt::Debug for Op {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		macro_rules! hex {
			($([$name:ident $($field:ident)*])*) => {
				match self {
					$(
						Op::$name { $($field,)* } => f
							.debug_struct(stringify!($name))
							$(.field(stringify!($field), &format_args!("{:#x}", $field)))*
							.finish(),
					)*
					&Op::Remount { cache_size } => f
						.debug_struct("Remount")
						.field("cache_size", &format_args!("{:#x?}", cache_size))
						.finish(),
				}
			};
		}
		hex! {
			[Create size]
			[Write idx offset amount]
			[Read idx offset amount]
			[Move from_idx to_idx]
			[Resize idx size]
			[Destroy idx]
		}
	}
}

impl<'a> Arbitrary<'a> for Test {
	fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
		// Always start with a create.
		let create_op = Op::Create { size: u.arbitrary()? };

		let &[a, b, c] = u.bytes(3)? else { unreachable!() };
		let cache_size = u32::from_le_bytes([a, b, c, 0]) as usize;

		Ok(Self::new(
			1 << 16,
			cache_size,
			[Ok(create_op)]
				.into_iter()
				.chain(u.arbitrary_iter::<Op>()?)
				.try_collect::<Box<_>>()?,
		))
	}
}

impl Test {
	pub fn new(blocks: usize, global_cache: usize, ops: impl Into<Vec<Op>>) -> Self {
		Self {
			store: new_cap(MaxRecordSize::K1, blocks, global_cache),
			ops: ops.into(),
			ids: Default::default(),
			contents: Default::default(),
		}
	}

	pub fn run(mut self) {
		self.ops.reverse();
		while !self.ops.is_empty() {
			let bg = Background::default();
			let mut new_cache_size = 0;
			run2(&bg, async {
				while let Some(op) = self.ops.pop() {
					match op {
						Op::Create { size } => {
							let obj = self.store.create(&bg).await.unwrap();
							obj.resize(size).await.unwrap();
							self.contents.insert(obj.id(), Default::default());
							self.ids.push(obj.id());
						}
						Op::Write { idx, offset, amount } => {
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
						}
						Op::Read { idx, offset, amount } => {
							let id = self.ids[idx as usize % self.ids.len()];
							let obj = self.store.get(&bg, id).await.unwrap();
							let len = obj.len().await.unwrap();
							if len > 0 {
								let offt = offset % len;
								let buf = &mut vec![0; amount.into()];
								let l = obj.read(offt, buf).await.unwrap();
								if l > 0 {
									let map = self.contents.get(&id).unwrap();
									for (i, c) in
										(offt..offt + u64::try_from(l).unwrap()).zip(&*buf)
									{
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
						}
						Op::Remount { cache_size: [a, b, c] } => {
							let f = usize::from;
							new_cache_size = f(a) | f(b) << 8 | f(c) << 16;
							break;
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
								.get(&bg, to_id)
								.await
								.unwrap()
								.replace_with(self.store.get(&bg, from_id).await.unwrap())
								.await
								.unwrap();

							let c = self.contents.remove(&from_id).unwrap();
							self.contents.insert(to_id, c);
						}
						Op::Resize { idx, size } => {
							let id = self.ids[idx as usize % self.ids.len()];
							let obj = self.store.get(&bg, id).await.unwrap();
							obj.resize(size).await.unwrap();
							if size < u64::MAX {
								self.contents.get_mut(&id).unwrap().remove(size..u64::MAX);
							}
						}
						Op::Destroy { idx } => {
							let i = idx as usize % self.ids.len();
							let id = self.ids[i];
							let obj = self.store.get(&bg, id).await.unwrap();
							obj.decrease_reference_count().await.unwrap();
							self.ids.swap_remove(i);
							// Stop immediately if no objects remain.
							if self.ids.is_empty() {
								self.ops.clear();
								break;
							}
						}
					}
				}
			});
			block_on(bg.drop()).unwrap();
			self.store = block_on(async {
				let devs = self.store.unmount().await.unwrap();
				Nros::load(StdResource::new(), devs, new_cache_size, true)
					.await
					.unwrap()
			});
		}
	}
}

use Op::*;

#[test]
fn unset_allocator_lba() {
	Test::new(
		512,
		4096,
		[
			Create { size: 18446744073709486123 },
			Remount { cache_size: [0, 16, 0] },
		],
	)
	.run()
}

#[test]
fn allocator_save_space_leak() {
	Test::new(
		512,
		4096,
		[
			Create { size: 18446744073709546299 },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
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
			Write { idx: 0, offset: 6917529024946200575, amount: 24415 },
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
			Remount { cache_size: [0, 16, 0] },
			Create { size: 5425430176097894400 },
			Write { idx: 1, offset: 21193410011155275, amount: 19275 },
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
			Read { idx: 0, offset: 18446744073709551509, amount: 38155 },
			Remount { cache_size: [0, 16, 0] },
			Remount { cache_size: [0, 16, 0] },
			Read { idx: 0, offset: 697696064, amount: 0 },
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
			Write { idx: 0, offset: 18446744073709551615, amount: 24575 },
			Write { idx: 0, offset: 71777215877963615, amount: 17247 },
			Create { size: 18446743382226067295 },
			Move { from_idx: 1, to_idx: 0 },
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
			Write { idx: 0, offset: 18446742978491529529, amount: 65535 },
			Resize { idx: 0, size: 18301847378652561407 },
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
			Write { idx: 0, offset: 18446743984320625663, amount: 65535 },
			Resize { idx: 0, size: 16947046754988065023 },
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
			Write { idx: 0, offset: 4123390705508810553, amount: 65535 },
			Resize { idx: 0, size: 1889604303433841151 },
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
			Remount { cache_size: [0, 16, 0] },
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
			Remount { cache_size: [0, 16, 0] },
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
			Write { idx: 0, offset: 16441494229869395940, amount: 11236 },
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
			Write { idx: 0, offset: 15987206784517266935, amount: 56797 },
			Resize { idx: 0, size: 15987205831607189504 },
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
			Remount { cache_size: [0, 16, 0] },
			Resize { idx: 0, size: 1025 },
			Remount { cache_size: [0, 16, 0] },
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
			Remount { cache_size: [0, 16, 0] },
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
			Remount { cache_size: [0, 16, 0] },
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
			Write { idx: 0, offset: 18446742974197925947, amount: 65535 },
			Remount { cache_size: [0, 16, 0] },
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
			Remount { cache_size: [0, 16, 0] },
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
			Write { idx: 0, offset: 17892238369727643649, amount: 20220 },
			Remount { cache_size: [0, 16, 0] },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn shrink_clear_too_many_children() {
	Test::new(
		1 << 16,
		1 << 10,
		[
			Create { size: 1 << 12 },
			Resize { idx: 0, size: 1 << 11 },
			Resize { idx: 0, size: 1 },
		],
	)
	.run()
}

/// Slots that are present *and* referenced also need to keep a Busy struct around
/// for the tasks that reference it.
#[test]
fn slot_present_moved() {
	Test::new(
		1 << 16,
		1152,
		[
			Create { size: 650152005029454 },
			Create { size: 1026497204047 },
			Resize { idx: 1, size: 16261110305688431531 },
			Move { from_idx: 1, to_idx: 0 },
		],
	)
	.run()
}

/// unmount() didn't poll the background runner it created.
#[test]
fn unmount_no_background_poll() {
	Test::new(
		1 << 16,
		3862,
		[
			Create { size: 18377782021482283007 },
			Write { idx: 0, offset: 10809483180534595583, amount: 33153 },
			Resize { idx: 0, size: 9337654175918632573 },
			Remount { cache_size: [0, 16, 0] },
		],
	)
	.run()
}

#[test]
fn move_object_busy_to() {
	Test::new(
		1 << 16,
		1184,
		[
			Create { size: 270751411552 },
			Create { size: 0 },
			Write { idx: 0, offset: 0, amount: 24576 },
			Create { size: 18424702927297969920 },
			Move { from_idx: 1, to_idx: 0 },
		],
	)
	.run()
}

/// Growing zero-sized trees simply involves changing the length.
#[test]
fn grow_zero_sized() {
	Test::new(
		1 << 16,
		1 << 20,
		[
			Create { size: 1 },
			Resize { idx: 0, size: 0 },
			Resize { idx: 0, size: 16 },
			Read { idx: 0, offset: 0, amount: 16 },
		],
	)
	.run()
}

#[test]
fn flush_all_skip_pseudo_id() {
	Test::new(
		1 << 16,
		1 << 20,
		[
			Create { size: 0 },
			Create { size: 1 << 63 },
			Resize { idx: 1, size: 1 << 60 },
			Write { idx: 1, offset: 0, amount: 1 },
			Remount { cache_size: [0, 16, 0] },
			Read { idx: 1, offset: 0, amount: 1 },
		],
	)
	.run()
}

/// Helper methods are important!
#[test]
fn root_leaky_dirty_marker() {
	Test::new(
		1 << 16,
		1279,
		[
			Create { size: 2442431929820581 },
			Resize { idx: 0, size: 47106071210353 },
			Resize { idx: 0, size: 11936128519798521855 },
			Resize { idx: 0, size: 42405 },
			Resize { idx: 0, size: 0 },
		],
	)
	.run()
}

#[test]
fn flush_all_clear_object_dirty_status() {
	Test::new(
		1 << 16,
		4359,
		[
			Create { size: 18446191058167073024 },
			Create { size: 651061555542690057 },
			Create { size: 4253941257439086857 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 672819019330704725 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 11315585473156024585 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 6220972285269444873 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Write { idx: 85, offset: 651061555542690134, amount: 2313 },
			Create { size: 651061555542690057 },
			Create { size: 651061555542690057 },
			Remount { cache_size: [0, 16, 0] },
		],
	)
	.run()
}

/// Don't blindly skip ahead if there are zero records in parent records,
/// there may be *dirty* non-zero records below.
#[test]
fn tree_fetch_entry_zero_skips_dirty() {
	Test::new(
		1 << 16,
		1109,
		[
			Create { size: 82829887134496 },
			Write { idx: 0, offset: 38019807206645, amount: 22015 },
			Write { idx: 0, offset: 38019801636085, amount: 21845 },
			Read { idx: 0, offset: 38019801636085, amount: 1000 },
		],
	)
	.run()
}

#[test]
fn pseudo_obj_leak_0() {
	Test::new(
		1 << 16,
		1152,
		[
			Create { size: 299489375270469631 },
			Create { size: 8399089676820909824 },
			Write { idx: 1, offset: 1945555036615554048, amount: 36751 },
			Write { idx: 1, offset: 1214522768312295936, amount: 63993 },
			Move { from_idx: 0, to_idx: 1 },
			Remount { cache_size: [0, 16, 0] },
		],
	)
	.run()
}

#[test]
fn write_zeros_offset_out_of_range() {
	Test::new(
		1 << 16,
		1361,
		[
			Create { size: 18374686479973133664 },
			Remount { cache_size: [0, 16, 0] },
			Write { idx: 0, offset: 18446462757646641112, amount: 65535 },
			Write { idx: 0, offset: 8110804808010534912, amount: 36751 },
			Write { idx: 0, offset: 18374687579183316818, amount: 65535 },
			Resize { idx: 0, size: 618465397503 },
		],
	)
	.run()
}

#[test]
fn use_after_free_0() {
	Test::new(
		1 << 16,
		1 << 10,
		[
			Create { size: 3329111198611275777 },
			Write { idx: 0, offset: 557601933144439097, amount: 54271 },
			Write { idx: 0, offset: 4412750543122677053, amount: 15677 },
			Write { idx: 0, offset: 4412750543122677053, amount: 15677 },
			Write { idx: 0, offset: 4412964684131403709, amount: 65341 },
			Resize { idx: 0, size: 72057594054639871 },
			Resize { idx: 0, size: 2285700095 },
		],
	)
	.run()
}

#[test]
fn pseudo_obj_leak_1() {
	Test::new(
		1 << 16,
		1024,
		[
			Create { size: 0x2004000 },
			Write { idx: 0, offset: 0x2, amount: 0xffff }, // 0x10001
			Write { idx: 0, offset: 0xf600, amount: 0x2100 }, // 0x11700
			Resize { idx: 0, size: 0xffff },               // 0xffff (erase 0x10000 to 0x11700)
		],
	)
	.run()
}

#[test]
fn pseudo_obj_leak_2() {
	Test::new(
		1 << 16,
		1428,
		[
			Create { size: 0x14000 },
			Write { idx: 0, offset: 0xd300, amount: 0x5300 },
			Write { idx: 0, offset: 0x7ffb, amount: 0xffff },
			Resize { idx: 0, size: 0x401 },
		],
	)
	.run()
}

#[test]
fn use_after_free_1() {
	Test::new(
		1 << 16,
		1085,
		[
			Create { size: 0x429ef0040000000 },
			Write { idx: 0, offset: 0xffffffffffffff, amount: 0x102 },
			Resize { idx: 0, size: 0xa5fffaffffffff },
			Resize { idx: 0, size: 0x4900000000009002 },
			Write { idx: 0, offset: 0xfff9ffffffffffff, amount: 0xffff },
			Resize { idx: 0, size: 0xfffffff9ffffff },
		],
	)
	.run()
}

#[test]
fn pseudo_obj_leak_3() {
	Test::new(
		1 << 16,
		1 << 24,
		[
			Create { size: 0xffffffffffffff1e },
			Write { idx: 0, offset: 0x4ffff, amount: 0xff00 },
			Remount { cache_size: [0, 16, 0] },
			Resize { idx: 0, size: 0xf420fb6c201ffff },
			Resize { idx: 0, size: 0xff49493dffffffff },
			Write { idx: 0, offset: 0x47ffffffff494949, amount: 0x9fff },
			Resize { idx: 0, size: 0xf420fb6c201ffff },
			Resize { idx: 0, size: 0x0 },
		],
	)
	.run()
}

#[test]
fn tree_get_chain_hop_off_by_one() {
	Test::new(
		1 << 16,
		255,
		[
			Create { size: 0x2000001 },
			Write { idx: 0, offset: 0x817000, amount: 0x2000 },
			Write { idx: 0, offset: 0x817000, amount: 0x2000 },
			Resize { idx: 0, size: 0x820000 },
		],
	)
	.run()
}

#[test]
fn update_record_replace_root_parent_depth_check() {
	Test::new(
		1 << 16,
		0,
		[
			Create { size: 0xd92300f407000000 },
			Resize { idx: 0, size: 0xff },
			Resize { idx: 0, size: 0xffffffffffffffff },
			Resize { idx: 0, size: 0x400 },
			Resize { idx: 0, size: 0x2fff },
		],
	)
	.run()
}

#[test]
fn move_object_update_record_stale_self_id() {
	Test::new(
		1 << 16,
		1 << 20,
		[
			Create { size: 0 },
			Create { size: 0x100000 },
			Remount { cache_size: [89, 0x0, 0x0] },
			Write { idx: 1, offset: 0, amount: 0x3801 },
			Write { idx: 1, offset: 0, amount: 0x3801 },
			Move { from_idx: 1, to_idx: 0 },
			Resize { idx: 0, size: 0x8000 },
		],
	)
	.run()
}

#[test]
fn grow_shrink_grow_chain_update_record_retry() {
	Test::new(
		1 << 16,
		0,
		[
			Create { size: 1 },
			Write { idx: 0, offset: 0, amount: 1 },
			Resize { idx: 0, size: 0x8000 },
			Resize { idx: 0, size: 0x400 },
			Resize { idx: 0, size: 0x8000 },
		],
	)
	.run()
}

#[test]
fn create_many_busy_object() {
	Test::new(
		1 << 16,
		0,
		[
			Create { size: 0 },
			Create { size: 0 },
			Destroy { idx: 0 },
			Create { size: 0 },
		],
	)
	.run()
}
