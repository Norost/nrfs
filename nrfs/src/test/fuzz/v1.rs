use {
	super::*,
	arbitrary::{Arbitrary, Unstructured},
	rangemap::RangeSet,
	std::collections::BTreeSet,
};

#[derive(Debug)]
struct Ref<'a> {
	key: ItemKey,
	name: Option<&'a Name>,
	ext: ItemExt,
	enabled_ext: EnableExt,
	data: RangeSet<u64>,
	len: u64,
	children: BTreeSet<&'a Name>,
	parent: u8,
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

#[derive(Debug, Arbitrary)]
pub enum Op<'a> {
	/// Create a file.
	CreateFile { dir_idx: u8, name: &'a Name, ext: ItemExt },
	/// Create a directory.
	CreateDir { dir_idx: u8, name: &'a Name, enable_ext: EnableExt, ext: ItemExt },
	/// Get an entry.
	Search { dir_idx: u8, name: &'a Name },
	/// Write to a file.
	Write { file_idx: u8, offset: u32, amount: u16 },
	/// Write to a file, growing it if necessary.
	WriteGrow { file_idx: u8, offset: u32, amount: u16 },
	/// Read from a file.
	Read { file_idx: u8, offset: u32, amount: u16 },
	/// Resize a file.
	Resize { file_idx: u8, len: u32 },
	/// Transfer an item.
	Transfer { idx: u8, to_dir_idx: u8, to: &'a Name },
	/// Destroy an item.
	Destroy { idx: u8 },
	/// Set `unix` extension data.
	SetExtUnix { idx: u8, ext: ext::Unix },
	/// Set `mtime` extension data.
	SetExtMtime { idx: u8, ext: ext::MTime },
	/// Get and verify extension data.
	GetExt { idx: u8 },
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

	pub fn run(mut self) {
		self.map.push(Some(Ref {
			key: ItemKey::Dir(self.fs.root_dir().into_key()),
			name: None,
			ext: Default::default(),
			enabled_ext: Default::default(),
			data: Default::default(),
			len: Default::default(),
			children: Default::default(),
			parent: u8::MAX,
		}));
		macro_rules! get {
			($idx:expr) => {{
				let Some(Some(r)) = self.map.get_mut(usize::from($idx)) else { continue };
				r
			}};
		}
		let mask_ext = |ext: ItemExt, enabled_ext: EnableExt| ItemExt {
			unix: enabled_ext.unix().then(|| ext.unix.unwrap_or_default()),
			mtime: enabled_ext.mtime().then(|| ext.mtime.unwrap_or_default()),
		};
		run(&self.fs, async {
			for op in self.ops.into_vec() {
				match op {
					Op::CreateFile { dir_idx, name, ext } => {
						let r = get!(dir_idx);
						let ItemKey::Dir(dir) = r.key else { continue };
						let dir = self.fs.dir(dir);

						match dir.create_file(name, ext.clone()).await.unwrap() {
							Ok(file) => {
								let ext = mask_ext(ext, r.enabled_ext);
								let new = r.children.insert(name);
								assert!(new, "name already present");
								self.map.push(Some(Ref {
									key: ItemKey::File(file.into_key()),
									name: Some(name),
									ext,
									enabled_ext: Default::default(),
									data: Default::default(),
									len: Default::default(),
									children: Default::default(),
									parent: dir_idx,
								}));
							}
							Err(CreateError::Duplicate) => {
								assert!(r.children.contains(name), "name not present");
							}
							Err(CreateError::Full) => {
								todo!();
							}
						}
					}
					Op::CreateDir { dir_idx, name, enable_ext, ext } => {
						let r = get!(dir_idx);
						let ItemKey::Dir(dir) = r.key else { continue };
						let dir = self.fs.dir(dir);

						match dir.create_dir(name, enable_ext, ext.clone()).await.unwrap() {
							Ok(dir) => {
								let ext = mask_ext(ext, r.enabled_ext);
								let new = r.children.insert(name);
								assert!(new, "name already present");
								self.map.push(Some(Ref {
									key: ItemKey::Dir(dir.into_key()),
									name: Some(name),
									ext,
									enabled_ext: enable_ext,
									data: Default::default(),
									len: Default::default(),
									children: Default::default(),
									parent: dir_idx,
								}));
							}
							Err(CreateError::Duplicate) => {
								assert!(r.children.contains(name), "name not present");
							}
							Err(CreateError::Full) => {
								todo!();
							}
						}
					}
					Op::Search { dir_idx, name } => {
						let r = get!(dir_idx);
						let ItemKey::Dir(dir) = r.key else { continue };
						let dir = self.fs.dir(dir);

						if let Some(_item) = dir.search(name).await.unwrap() {
							assert!(r.children.contains(name), "item shouldn't be present");
						} else {
							assert!(!r.children.contains(name), "item should be present");
						}
					}
					Op::Write { file_idx, offset, amount } => {
						let r = get!(file_idx);
						let ItemKey::File(file) = r.key else { continue };
						let file = self.fs.file(file);

						let offset = u64::from(offset);
						let end = (offset + u64::from(amount)).min(r.len);
						let len = file.write(offset, &vec![1; amount.into()]).await.unwrap();

						if len == 0 {
							assert!(amount == 0 || offset >= r.len, "unexpected amount written");
						} else {
							assert_eq!(end - offset, len as u64, "unexpected amount written");
						}
						if len > 0 && offset != end {
							r.data.insert(offset..end);
						}
					}
					Op::WriteGrow { file_idx, offset, amount } => {
						let r = get!(file_idx);
						let ItemKey::File(file) = r.key else { continue };
						let file = self.fs.file(file);

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
								assert!(end > self.fs.storage.obj_max_len());
							}
						}
					}
					Op::Read { file_idx, offset, amount } => {
						let r = get!(file_idx);
						let ItemKey::File(file) = r.key else { continue };
						let file = self.fs.file(file);

						let offset = u64::from(offset);
						let end = (offset + u64::from(amount)).min(r.len);
						let buf = &mut vec![2; amount.into()];
						let len = file.read(offset, buf).await.unwrap();

						if len == 0 {
							assert!(amount == 0 || offset >= r.len, "unexpected amount read");
						} else {
							assert_eq!(offset + len as u64, end, "unexpected amount read");
						}
						for (i, c) in (offset..end).zip(&buf[..len]) {
							assert_eq!(u8::from(r.data.contains(&i)), *c, "data mismatch");
						}
					}
					Op::Resize { file_idx, len } => {
						let r = get!(file_idx);
						let ItemKey::File(file) = r.key else { continue };
						let file = self.fs.file(file);

						match file.resize(len.into()).await.unwrap() {
							Ok(()) => {
								r.len = len.into();
								r.data.remove(len.into()..u64::MAX);
							}
							Err(LengthTooLong) => {
								assert!(u64::from(len) > self.fs.storage.obj_max_len());
							}
						}
					}
					Op::Transfer { idx, to_dir_idx, to } => {
						let to_r = get!(to_dir_idx);
						let ItemKey::Dir(to_dir) = to_r.key else { continue };
						let to_dir = self.fs.dir(to_dir);
						let to_enabled_ext = to_r.enabled_ext;

						let r = get!(idx);
						match self.fs.item(r.key).transfer(&to_dir, to).await.unwrap() {
							Ok(key) => {
								r.key = key;

								let e = to_enabled_ext;
								r.ext.unix = e.unix().then(|| r.ext.unix.unwrap_or_default());
								r.ext.mtime = e.mtime().then(|| r.ext.mtime.unwrap_or_default());

								let parent = r.parent;
								r.parent = to_dir_idx;

								let name = r.name.replace(to);
								if let Some(name) = name {
									let from_r = get!(parent);
									let removed = from_r.children.remove(name);
									assert!(removed, "{:?} was not present", name);
								}

								let to_r = get!(to_dir_idx);
								let new = to_r.children.insert(to);
								assert!(new, "{:?} was not present", to);
							}
							Err(TransferError::Full) => todo!(),
							Err(TransferError::Duplicate) => {
								let to_r = get!(to_dir_idx);
								assert!(to_r.children.contains(to), "no item with name");
							}
							Err(TransferError::IsRoot) => {
								assert_eq!(idx, 0, "not root");
							}
						}
					}
					Op::Destroy { idx } => {
						let r = get!(idx);

						match r.key {
							ItemKey::Dir(k) => {
								match crate::Dir::new(&self.fs, k).destroy().await.unwrap() {
									Ok(()) => {}
									Err(DirDestroyError::NotEmpty) => {
										assert!(!r.children.is_empty(), "dir is empty");
										continue;
									}
									Err(DirDestroyError::IsRoot) => {
										assert_eq!(idx, 0, "not root");
										continue;
									}
								}
							}
							ItemKey::File(k) | ItemKey::Sym(k) => {
								crate::File::new(&self.fs, k).destroy().await.unwrap();
							}
						}
						if let Some(name) = r.name {
							let parent = r.parent;
							let parent = get!(parent);
							let removed = parent.children.remove(name);
							assert!(removed, "{:?} was not present", name);
						}
						self.map[usize::from(idx)] = None;
					}
					Op::SetExtUnix { idx, ext } => {
						let r = get!(idx);
						let item = self.fs.item(r.key);
						let res = item.set_unix(ext).await.unwrap();
						r.ext.unix = res.then(|| ext);
					}
					Op::SetExtMtime { idx, ext } => {
						let r = get!(idx);
						let item = self.fs.item(r.key);
						let res = item.set_mtime(ext).await.unwrap();
						r.ext.mtime = res.then(|| ext);
					}
					Op::GetExt { idx } => {
						let r = get!(idx);
						let item = self.fs.item(r.key);
						let ext = item.ext().await.unwrap();

						//assert_eq!(r.enabled_ext.unix(), ext.unix.is_some());
						//assert_eq!(r.enabled_ext.mtime(), ext.mtime.is_some());
						match (ext.unix, r.ext.unix) {
							(Some(l), Some(r)) => {
								let l = (l.permissions, l.uid(), l.gid());
								let r = (r.permissions, r.uid(), r.gid());
								assert_eq!(l, r);
							}
							(None, None) => {}
							(l, r) => panic!("unix ext mismatch {:?} <> {:?}", l, r),
						}
						match (ext.mtime, r.ext.mtime) {
							(Some(l), Some(r)) => {
								assert_eq!(l.mtime, r.mtime);
							}
							(None, None) => {}
							(l, r) => panic!("mtime ext mismatch {:?} <> {:?}", l, r),
						}
					}
				}
			}
		});
	}
}
