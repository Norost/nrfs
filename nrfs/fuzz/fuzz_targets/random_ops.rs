#![no_main]
#![deny(unused_must_use)]
#![feature(iterator_try_collect)]

use {
	libfuzzer_sys::{
		arbitrary::{self, Arbitrary, Unstructured},
		fuzz_target,
	},
	nrfs::{dev::MemDev, *},
	rangemap::RangeSet,
	std::{
		collections::BTreeMap,
		future::Future,
		task::{Context, Poll},
	},
};

fn block_on<R>(fut: impl Future<Output = R>) -> R {
	let mut fut = core::pin::pin!(fut);
	let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
	loop {
		if let Poll::Ready(r) = fut.as_mut().poll(&mut cx) {
			return r;
		}
	}
}

fn new_cap(size: usize, block_size: BlockSize, max_record_size: MaxRecordSize) -> Nrfs<MemDev> {
	block_on(Nrfs::new(NewConfig {
		key_deriver: KeyDeriver::None { key: &[0; 32] },
		cipher: CipherType::NoneXxh3,
		mirrors: vec![vec![MemDev::new(size, block_size)]],
		block_size,
		max_record_size,
		compression: Compression::None,
		cache_size: 4096,
	}))
	.unwrap()
}

#[derive(Debug)]
struct RefDir<'a> {
	key: ItemKey,
	name: &'a Key,
	children: BTreeMap<&'a Key, u8>,
	parent: u8,
}

#[derive(Debug)]
struct RefFile<'a> {
	key: ItemKey,
	name: &'a Key,
	data: RangeSet<u64>,
	len: u64,
	parent: u8,
}

#[derive(Debug)]
enum Ref<'a> {
	Dir(RefDir<'a>),
	File(RefFile<'a>),
}

macro_rules! f {
	($f:ident $t:ty) => {
		fn $f(&mut self) -> &mut $t {
			match self {
				Self::Dir(d) => &mut d.$f,
				Self::File(f) => &mut f.$f,
			}
		}
	};
}

impl<'a> Ref<'a> {
	f!(name &'a Key);
	f!(parent u8);
	f!(key ItemKey);
}

#[derive(Debug)]
pub struct Test<'a> {
	/// Filesystem to operate on.
	fs: Nrfs<MemDev>,
	/// Ops to execute
	ops: Box<[Op<'a>]>,
	/// Mapping from IDs to item keys & paths.
	map: Vec<Option<Ref<'a>>>,
}

#[derive(Clone, Debug, Arbitrary)]
pub enum Op<'a> {
	/// Create a file.
	CreateFile { dir_idx: u8, name: &'a Key },
	/// Create a directory.
	CreateDir { dir_idx: u8, name: &'a Key },
	/// Get an entry.
	Search { dir_idx: u8, name: &'a Key },
	/// Write to a file.
	Write { file_idx: u8, offset: u32, amount: u16 },
	/// Write to a file, growing it if necessary.
	WriteGrow { file_idx: u8, offset: u32, amount: u16 },
	/// Read from a file.
	Read { file_idx: u8, offset: u32, amount: u16 },
	/// Resize a file.
	Resize { file_idx: u8, len: u32 },
	/// Transfer an item.
	Transfer { idx: u8, to_idx: u8, to: &'a Key },
	/// Remove an item.
	Remove { idx: u8 },
	/// Remount filesystem.
	Remount,
}

impl<'a> Arbitrary<'a> for Test<'a> {
	fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
		Ok(Self::new(
			1 << 16,
			u.arbitrary_iter::<Op<'a>>()?.try_collect::<Box<_>>()?,
		))
	}
}

impl<'a> Test<'a> {
	pub fn new(blocks: usize, ops: impl Into<Box<[Op<'a>]>>) -> Self {
		Self {
			fs: new_cap(blocks, BlockSize::K1, MaxRecordSize::K1),
			ops: ops.into(),
			map: Default::default(),
		}
	}

	pub async fn run(mut self) {
		self.map.push(Some(Ref::Dir(RefDir {
			key: self.fs.root_dir().key(),
			name: b"\0".into(),
			children: Default::default(),
			parent: u8::MAX,
		})));
		macro_rules! get {
			($idx:expr) => {{
				let Some(Some(r)) = self.map.get_mut(usize::from($idx)) else { continue };
				r
			}};
		}

		let mut ops = self.ops.iter().cloned().peekable();

		while ops.peek().is_some() {
			self.fs
				.run(async {
					while let Some(op) = ops.next() {
						match op {
							Op::CreateFile { dir_idx, name } => {
								let idx = self.map.len().try_into().unwrap();
								let Ref::Dir(r) = get!(dir_idx) else { continue };
								let dir = self.fs.dir(r.key).await.unwrap();

								match dir.create_file(name).await.unwrap() {
									Ok(file) => {
										let prev = r.children.insert(name, idx);
										assert!(prev.is_none(), "name already present");
										self.map.push(Some(Ref::File(RefFile {
											key: file.key(),
											name,
											data: Default::default(),
											len: 0,
											parent: dir_idx,
										})));
									}
									Err(CreateError::Duplicate) => {
										assert!(r.children.contains_key(name), "name not present");
									}
									Err(CreateError::Full) => todo!(),
								}
							}
							Op::CreateDir { dir_idx, name } => {
								let idx = self.map.len().try_into().unwrap();
								let Ref::Dir(r) = get!(dir_idx) else { continue };
								let dir = self.fs.dir(r.key).await.unwrap();

								match dir.create_dir(name).await.unwrap() {
									Ok(dir) => {
										let prev = r.children.insert(name, idx);
										assert!(prev.is_none(), "name already present");
										self.map.push(Some(Ref::Dir(RefDir {
											key: dir.key(),
											name,
											children: Default::default(),
											parent: dir_idx,
										})));
									}
									Err(CreateError::Duplicate) => {
										assert!(r.children.contains_key(name), "name not present");
									}
									Err(CreateError::Full) => todo!(),
								}
							}
							Op::Search { dir_idx, name } => {
								let Ref::Dir(r) = get!(dir_idx) else { continue };
								let dir = self.fs.dir(r.key).await.unwrap();

								if let Some(_item) = dir.search(name).await.unwrap() {
									assert!(
										r.children.contains_key(name),
										"item shouldn't be present"
									);
								} else {
									assert!(
										!r.children.contains_key(name),
										"item should be present"
									);
								}
							}
							Op::Write { file_idx, offset, amount } => {
								let Ref::File(r) = get!(file_idx) else { continue };
								let file = self.fs.file(r.key);

								let offset = u64::from(offset);
								let end = (offset + u64::from(amount)).min(r.len);
								let len =
									file.write(offset, &vec![1; amount.into()]).await.unwrap();

								if len == 0 {
									assert!(
										amount == 0 || offset >= r.len,
										"unexpected amount written"
									);
								} else {
									assert_eq!(
										end - offset,
										len as u64,
										"unexpected amount written"
									);
								}
								if len > 0 && offset != end {
									r.data.insert(offset..end);
								}
							}
							Op::WriteGrow { file_idx, offset, amount } => {
								let Ref::File(r) = get!(file_idx) else { continue };
								let file = self.fs.file(r.key);

								let offset = u64::from(offset);
								let end = offset + u64::from(amount);
								match file
									.write_grow(offset, &vec![1; amount.into()])
									.await
									.unwrap()
								{
									Ok(()) => {
										if offset != end {
											r.len = r.len.max(end);
											r.data.insert(offset..end);
										}
									}
									Err(LengthTooLong) => {
										assert!(end > self.fs.max_len());
									}
								}
							}
							Op::Read { file_idx, offset, amount } => {
								let Ref::File(r) = get!(file_idx) else { continue };
								let file = self.fs.file(r.key);

								let offset = u64::from(offset);
								let end = (offset + u64::from(amount)).min(r.len);
								let buf = &mut vec![2; amount.into()];
								let len = file.read(offset, buf).await.unwrap();

								if len == 0 {
									assert!(
										amount == 0 || offset >= r.len,
										"unexpected amount read"
									);
								} else {
									assert_eq!(offset + len as u64, end, "unexpected amount read");
								}
								for (i, c) in (offset..end).zip(&buf[..len]) {
									assert_eq!(u8::from(r.data.contains(&i)), *c, "data mismatch");
								}
							}
							Op::Resize { file_idx, len } => {
								let Ref::File(r) = get!(file_idx) else { continue };
								match self.fs.file(r.key).resize(len.into()).await.unwrap() {
									Ok(()) => {
										r.len = len.into();
										r.data.remove(len.into()..u64::MAX);
									}
									Err(LengthTooLong) => {
										assert!(u64::from(len) > self.fs.max_len());
									}
								}
							}
							Op::Transfer { idx, to_idx, to } => {
								if idx == 0 {
									continue;
								}
								let r = get!(idx);
								let (parent, key, name) = (*r.parent(), *r.key(), *r.name());
								let Ref::Dir(to_r) = get!(to_idx) else { continue };
								let to_dir = self.fs.dir(to_r.key).await.unwrap();
								let Ref::Dir(from_r) = get!(parent) else { unreachable!() };
								let from_dir = self.fs.dir(from_r.key).await.unwrap();

								match from_dir.transfer(key, &to_dir, to).await.unwrap() {
									Ok(key) => {
										let idx =
											from_r.children.remove(name).expect("not present");
										*self.map[usize::from(idx)]
											.as_mut()
											.expect("invalid idx")
											.name() = to;
										let Ref::Dir(to_r) = get!(to_idx) else { unreachable!() };
										let prev = to_r.children.insert(to, idx);
										assert!(prev.is_none(), "already present");
										let r = get!(idx);
										(*r.parent(), *r.key(), *r.name()) = (to_idx, key, to);
									}
									Err(TransferError::Full) => todo!(),
									Err(TransferError::Duplicate) => {
										let Ref::Dir(to_r) = get!(to_idx) else { unreachable!() };
										assert!(
											to_r.children.contains_key(to),
											"no item with name"
										);
									}
								}
							}
							Op::Remove { idx } => {
								if idx == 0 {
									continue;
								}
								let r = get!(idx);
								let (parent, key, name) = (*r.parent(), *r.key(), *r.name());
								let Ref::Dir(dr) = get!(parent) else { unreachable!() };
								let d = self.fs.dir(dr.key).await.unwrap();
								match d.remove(key).await.unwrap() {
									Ok(()) => {
										let idx = dr.children.remove(name).expect("not present");
										let s = self.map[usize::from(idx)]
											.take()
											.expect("not referenced");
										if let Ref::Dir(s) = s {
											assert!(s.children.is_empty(), "dir is not empty");
										}
									}
									Err(RemoveError::NotEmpty) => {
										let &idx = dr.children.get(name).expect("not present");
										let s = self.map[usize::from(idx)]
											.as_ref()
											.expect("not referenced");
										let Ref::Dir(s) = s else { panic!("not a dir") };
										assert!(!s.children.is_empty(), "dir is empty");
									}
								}
							}
							Op::Remount => break,
						}
					}
					Ok::<_, Error<_>>(())
				})
				.await
				.unwrap();
			let devices = block_on(self.fs.unmount()).unwrap();
			self.fs = block_on(Nrfs::load(LoadConfig {
				devices,
				cache_size: 1 << 12,
				allow_repair: true,
				retrieve_key: &mut |_| unreachable!(),
			}))
			.unwrap();
		}
	}
}

fuzz_target!(|test: Test| {
	block_on(test.run());
});
