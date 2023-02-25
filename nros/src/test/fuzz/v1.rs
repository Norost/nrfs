use {
	super::*,
	arbitrary::{Arbitrary, Unstructured},
	rangemap::RangeSet,
	std::collections::BTreeMap,
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
	contents: BTreeMap<u64, RangeSet<u32>>,
}

#[derive(Arbitrary)]
pub enum Op {
	/// Create an object.
	Create,
	/// Write 1s to an object.
	///
	/// `idx` is and index in `ids`.
	/// `offset` is modulo size of object.
	Write { idx: u8, offset: u32, amount: u16 },
	/// Write 0s to an object.
	///
	/// `idx` is an index in `ids`.
	/// `offset` is modulo size of object.
	WriteZeros { idx: u8, offset: u32, amount: u32 },
	/// Read from an object.
	///
	/// `idx` is an index in `ids`.
	/// `offset` is modulo size of object.
	///
	/// Also verifies contents.
	Read { idx: u8, offset: u32, amount: u16 },
	/// Remount the filesystem.
	Remount { cache_size: [u8; 3] },
	/// Destroy an object.
	///
	/// If no objects remain, [`Test::run`] stops and unmounts the filesystem.
	Destroy { idx: u8 },
}

impl fmt::Debug for Op {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		macro_rules! hex {
			($([$name:ident $($field:ident)*])*) => {
				match self {
					$(
						Op::$name { $($field,)* } => f
							.debug_struct(stringify!($name))
							$(.field(stringify!($field), &format_args!("{:#x?}", $field)))*
							.finish(),
					)*
				}
			};
		}
		hex! {
			[Create]
			[Write idx offset amount]
			[WriteZeros idx offset amount]
			[Read idx offset amount]
			[Destroy idx]
			[Remount cache_size]
		}
	}
}

impl<'a> Arbitrary<'a> for Test {
	fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
		// Always start with a create.
		let create_op = Op::Create;

		let &[a, b, c] = u.bytes(3)? else { unreachable!() };
		let cache_size = u32::from_le_bytes([a, b, c, 0]) as usize;

		Ok(Self::new(
			cache_size,
			[Ok(create_op)]
				.into_iter()
				.chain(u.arbitrary_iter::<Op>()?)
				.try_collect::<Box<_>>()?,
		))
	}
}

impl Test {
	pub fn new(global_cache: usize, ops: impl Into<Vec<Op>>) -> Self {
		Self {
			store: new_cap(MaxRecordSize::K1, 1 << 16, global_cache),
			ops: ops.into(),
			ids: Default::default(),
			contents: Default::default(),
		}
	}

	pub fn run(mut self) {
		// Writes or reads beyond this limit should be truncated.
		let max_obj_size = (0..4).fold(0, |s, d| s + (1 << 10 + 7 * d));

		self.ops.reverse();
		while !self.ops.is_empty() {
			let mut new_cache_size = 0;
			run(&self.store, async {
				while let Some(op) = self.ops.pop() {
					match op {
						Op::Create => {
							let obj = self.store.create().await.unwrap();
							self.contents.insert(obj.id(), Default::default());
							self.ids.push(obj.id());
						}
						Op::Write { idx, offset, amount } => {
							let id = self.ids[idx as usize % self.ids.len()];
							let obj = self.store.get(id).await.unwrap();

							let l = obj
								.write(offset.into(), &vec![1; amount.into()])
								.await
								.unwrap();
							let l = u32::try_from(l).unwrap();

							let amount = u32::from(amount);
							let top = offset.saturating_add(amount).min(max_obj_size);
							assert_eq!(l, top.saturating_sub(offset), "unexpected write length");

							if offset < top {
								self.contents.get_mut(&id).unwrap().insert(offset..top);
							}
						}
						Op::WriteZeros { idx, offset, amount } => {
							let id = self.ids[idx as usize % self.ids.len()];
							let obj = self.store.get(id).await.unwrap();

							let l = obj.write_zeros(offset.into(), amount.into()).await.unwrap();
							let l = u32::try_from(l).unwrap();

							let top = offset.saturating_add(amount).min(max_obj_size);
							assert_eq!(
								l,
								top.saturating_sub(offset),
								"unexpected write zeros length"
							);

							if offset < top {
								self.contents.get_mut(&id).unwrap().remove(offset..top);
							}
						}
						Op::Read { idx, offset, amount } => {
							let id = self.ids[idx as usize % self.ids.len()];
							let obj = self.store.get(id).await.unwrap();

							let buf = &mut vec![2; amount.into()];
							let l = obj.read(offset.into(), buf).await.unwrap();
							let l = u32::try_from(l).unwrap();

							let amount = u32::from(amount);
							let top = offset.saturating_add(amount).min(max_obj_size);
							assert_eq!(l, top.saturating_sub(offset), "unexpected read length");

							if offset < top {
								let mut buf = &mut **buf;
								let map = self.contents.get(&id).unwrap();
								let mut prev = offset;

								let test = |buf: &[_], i, k, v| {
									let l = usize::try_from(k - i).unwrap();
									let chk = buf[..l].iter().all(|&b| b == v);
									assert!(chk, "expected all {}s between {:#x?}", v, i..k);
									l
								};

								for r in map.gaps(&(offset..top)) {
									if prev != r.start {
										let l = test(buf, prev, r.start, 1);
										buf = &mut buf[l..];
									}
									let l = test(buf, r.start, r.end, 0);
									buf = &mut buf[l..];
									prev = r.end;
								}
							}
						}
						Op::Remount { cache_size: [a, b, c] } => {
							let f = usize::from;
							new_cache_size = f(a) | f(b) << 8 | f(c) << 16;
							break;
						}
						Op::Destroy { idx } => {
							let i = idx as usize % self.ids.len();
							let id = self.ids[i];
							let obj = self.store.get(id).await.unwrap();
							obj.dealloc().await.unwrap();
							self.ids.swap_remove(i);
							// Stop immediately if no objects remain.
							if self.ids.is_empty() {
								self.ops.clear();
								break;
							}
						}
					}
				}
				Ok(())
			});

			self.store = block_on(async {
				let devices = self.store.unmount().await.unwrap();
				Nros::load(LoadConfig {
					resource: StdResource::new(),
					devices,
					cache_size: new_cache_size,
					allow_repair: true,
					magic: *b"TEST",
					retrieve_key: &mut |_| unreachable!(),
				})
				.await
				.unwrap()
			});
		}
	}
}

use Op::*;

#[test]
fn busy_evict_entry_fetch_race() {
	Test::new(
		255,
		[Create, Write { idx: 0, offset: 0x79ff60ff, amount: 0x68ff }],
	)
	.run()
}

#[test]
fn store_write_buf_size() {
	Test::new(
		33,
		[
			Create,
			Write { idx: 0, offset: 0x7400a50a, amount: 0xff04 },
			Write { idx: 0, offset: 0x211ad131, amount: 0xffcd },
			Destroy { idx: 0 },
		],
	)
	.run()
}

#[test]
fn write_zeros_upwards_bad_offset() {
	Test::new(
		12671999,
		[
			Create,
			Write { idx: 0, offset: 0x41414141, amount: 1 },
			Remount { cache_size: [0xaf, 0xaf, 0x41] },
			WriteZeros { idx: 0, offset: 0x1b414141, amount: 0x5d5d3f3f },
			Read { idx: 0, offset: 0x41414141, amount: 1 },
		],
	)
	.run()
}

/// Growing an object list didn't mark the newly added entry as dirty.
#[test]
fn object_list_grow_not_dirty() {
	Test::new(
		33,
		[
			Create, // 0
			Write { idx: 0, offset: 0x65656536, amount: 0x6565 },
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create, // 16
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create,
			Create, // 32
			Remount { cache_size: [0x39, 0x65, 0x65] },
			Create, // 33, needs grow
			Create,
			Create,
			Create,
			Remount { cache_size: [0x65, 0x65, 0x65] },
			Read { idx: 0, offset: 0x65656565, amount: 0x6565 },
		],
	)
	.run()
}

#[test]
fn write_zeros_missing_end() {
	Test::new(
		3014941,
		[
			Create,
			Write { idx: 0, offset: 0x400, amount: 1 },
			WriteZeros { idx: 0, offset: 0, amount: 0x800 },
			Read { idx: 0, offset: 0x400, amount: 1 },
		],
	)
	.run()
}
