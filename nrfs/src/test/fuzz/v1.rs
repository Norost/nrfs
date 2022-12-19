use {
	super::*,
	arbitrary::{Arbitrary, Unstructured},
	core::mem,
	rangemap::RangeSet,
	rustc_hash::{FxHashMap, FxHashSet},
	std::{collections::hash_map, rc::Rc},
};

// TODO make part of public API.
#[derive(Debug)]
enum RawEntryRef {
	File(RawFileRef),
	Dir(RawDirRef),
}

#[derive(Debug)]
pub struct Test<'a> {
	/// Filesystem to operate on.
	fs: Nrfs<MemDev>,
	/// Ops to execute
	ops: Box<[Op<'a>]>,
}

/// Expected state of an object somewhere in the filesystem tree.
#[derive(Debug)]
enum State<'a> {
	/// The object is a file.
	File { contents: RangeSet<u64> },
	/// The object is a directory.
	Dir { children: FxHashMap<&'a Name, State<'a>> },
}

impl<'a> State<'a> {
	fn file_mut(&mut self) -> &mut RangeSet<u64> {
		let Self::File { contents } = self else { panic!("not a file") };
		contents
	}

	fn dir_mut(&mut self) -> &mut FxHashMap<&'a Name, State<'a>> {
		let Self::Dir { children } = self else { panic!("not a dir") };
		children
	}
}

/// Get a mutable reference to a [`State`] node.
///
/// Fails if one of the nodes could not be found or is a [`State::File`].
fn state_mut<'a, 'b, I>(
	mut children: &'b mut FxHashMap<&'a Name, State<'a>>,
	path: I,
) -> Option<&'b mut State<'a>>
where
	I: IntoIterator<Item = &'a Name>,
{
	let mut path = path.into_iter().peekable();
	while path.peek().is_some() {
		match children.get_mut(path.next().unwrap())? {
			State::File { .. } => return None,
			State::Dir { children: c } => children = c,
		}
	}
	children.get_mut(path.next().unwrap())
}

#[derive(Debug, Arbitrary)]
pub enum Op<'a> {
	/// Create a file.
	CreateFile { dir_idx: u16, name: &'a Name },
	/// Create a directory.
	CreateDir { dir_idx: u16, name: &'a Name, key: [u8; 16] },
	/// Get an entry.
	Get { dir_idx: u16, name: &'a Name },
	/// Drop an entry.
	Drop { idx: u16 },
	/// Write to a file.
	Write { file_idx: u16, offset: u64, amount: u16 },
	/// Read from a file.
	Read { file_idx: u16, offset: u64, amount: u16 },
	/// Resize a file.
	Resize { file_idx: u16, len: u64 },
	/// Rename an entry.
	Rename { dir_idx: u16, from: &'a Name, to: &'a Name },
	/// Transfer an entry.
	Transfer { from_dir_idx: u16, from: &'a Name, to_dir_idx: u16, to: &'a Name },
	/// Destroy an entry.
	Destroy { idx: u16 },
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
			fs: run(new_cap(blocks, BlockSize::B512, MaxRecordSize::B512)),
			ops: ops.into(),
		}
	}

	pub fn run(mut self) {
		run(async {
			// References to entries.
			let mut refs = Vec::<(_, Box<[&Name]>)>::new();
			// Expected contents of the filesystem,
			let mut state = FxHashMap::default();

			fn get_dir<'a, 'b, 'c, 'd>(
				fs: &'b Nrfs<MemDev>,
				refs: &'c Vec<(RawEntryRef, Box<[&'a Name]>)>,
				state: &'d mut FxHashMap<&'a Name, State<'a>>,
				dir_idx: u16,
			) -> Option<(
				DirRef<'b, MemDev>,
				&'c [&'a Name],
				&'d mut FxHashMap<&'a Name, State<'a>>,
			)> {
				match refs.get(usize::from(dir_idx)) {
					Some((RawEntryRef::Dir(dir), path)) => {
						let d = state_mut(state, path.iter().copied()).unwrap().dir_mut();
						Some((DirRef::from_raw(fs, dir.clone()), path, d))
					}
					_ => None,
				}
			}

			fn get_file<'a, 'b, 'c, 'd>(
				fs: &'b Nrfs<MemDev>,
				refs: &'c Vec<(RawEntryRef, Box<[&'a Name]>)>,
				state: &'d mut FxHashMap<&'a Name, State<'a>>,
				file_idx: u16,
			) -> Option<(FileRef<'b, MemDev>, &'c [&'a Name], &'d mut RangeSet<u64>)> {
				match refs.get(usize::from(file_idx)) {
					Some((RawEntryRef::File(file), path)) => {
						let c = state_mut(state, path.iter().copied()).unwrap().file_mut();
						Some((FileRef::from_raw(fs, file.clone()), path, c))
					}
					_ => None,
				}
			}

			fn append<'a>(path: &[&'a Name], name: &'a Name) -> Box<[&'a Name]> {
				path.iter().copied().chain([name]).collect()
			}

			for op in self.ops.into_vec() {
				match op {
					Op::CreateFile { dir_idx, name } => {
						let Some((dir, _, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };

						if dir
							.create_file(name, &Default::default())
							.await
							.unwrap()
							.is_some()
						{
							let r = d.insert(name, State::File { contents: Default::default() });
							assert!(r.is_none());
						} else {
							assert!(d.contains_key(name));
						}

						let _ = dir.into_raw();
					}
					Op::CreateDir { dir_idx, name, key } => {
						let Some((dir, _, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };

						if dir
							.create_dir(name, &DirOptions::new(&key), &Default::default())
							.await
							.unwrap()
							.is_some()
						{
							let r = d.insert(name, State::Dir { children: Default::default() });
							assert!(r.is_none());
						} else {
							assert!(d.contains_key(name));
						}

						let _ = dir.into_raw();
					}
					Op::Get { dir_idx, name } => {
						let Some((dir, path, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };

						let path = append(path, name);
						if let Some(entry) = dir.find(name).await.unwrap() {
							assert!(d.contains_key(name));
							let r = match entry {
								Entry::File(e) => RawEntryRef::File(e.into_raw()),
								Entry::Dir(e) => RawEntryRef::Dir(e.into_raw()),
								_ => panic!("unexpected entry type"),
							};
							refs.push((r, path));
						} else {
							assert!(!d.contains_key(name));
						}

						let _ = dir.into_raw();
					}
					Op::Drop { idx } => {
						if usize::from(idx) < refs.len() {
							refs.swap_remove(idx.into());
						}
					}
					Op::Write { file_idx, offset, amount } => {
						let Some((file, _, contents)) = get_file(&self.fs, &refs, &mut state, file_idx) else { continue };

						// Truncate
						let end = offset.saturating_add(amount.into());
						let amount = u16::try_from(end - offset).unwrap();

						// Write
						file.write_grow(offset, &vec![1; amount.into()])
							.await
							.unwrap();

						// Update expected contents
						if amount > 0 {
							contents.insert(offset..offset + u64::from(amount));
						}

						let _ = file.into_raw();
					}
					Op::Read { file_idx, offset, amount } => {
						let Some((file, _, contents)) = get_file(&self.fs, &refs, &mut state, file_idx) else { continue };

						// Wrap offset
						let len = file.len().await.unwrap();
						let offt = offset % len;

						// Read
						let buf = &mut vec![0; amount.into()];
						let l = file.read(offt, buf).await.unwrap();

						// Verify contents
						if l > 0 {
							for (i, c) in (offt..offt + u64::try_from(l).unwrap()).zip(&*buf) {
								assert_eq!(contents.contains(&i), *c == 1);
							}
						}

						let _ = file.into_raw();
					}
					Op::Resize { file_idx, len } => {
						let Some((file, _, contents)) = get_file(&self.fs, &refs, &mut state, file_idx) else { continue };
						file.resize(len).await.unwrap();
						if len < u64::MAX {
							contents.remove(len..u64::MAX);
						}
						let _ = file.into_raw();
					}
					Op::Rename { dir_idx, from, to } => {
						let Some((dir, _, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };
						if dir.rename(from, to).await.unwrap() {
							let e = d.remove(from).unwrap();
							assert!(d.insert(to, e).is_none());
						} else {
							assert!(!d.contains_key(from) || d.contains_key(to));
						}
						let _ = dir.into_raw();
					}
					Op::Transfer { from_dir_idx, from, to_dir_idx, to } => {
						let Some((to_dir, to_path, _)) = get_dir(&self.fs, &refs, &mut state, to_dir_idx) else { continue };
						let Some((from_dir, _, d)) = get_dir(&self.fs, &refs, &mut state, from_dir_idx) else { continue };

						if from_dir.transfer(from, &to_dir, to).await.unwrap() {
							let e = d.remove(from).unwrap();
							let d = state_mut(&mut state, to_path.iter().copied()).unwrap();
							let r = d.dir_mut().insert(to, e);
							assert!(r.is_none())
						} else {
							let from_contains = d.contains_key(from);
							let d = state_mut(&mut state, to_path.iter().copied()).unwrap();
							assert!(!from_contains || d.dir_mut().contains_key(to));
						}

						let _ = from_dir.into_raw();
						let _ = to_dir.into_raw();
					}
					Op::Destroy { idx } => {
						// TODO
					}
				}
			}
		})
	}
}

use Op::*;
