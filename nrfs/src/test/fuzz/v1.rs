use {
	super::*,
	crate::dir::{DirSize, Hasher, RemoveError},
	arbitrary::{Arbitrary, Unstructured},
	rangemap::RangeSet,
	rustc_hash::FxHashMap,
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
	File {
		contents: RangeSet<u64>,
		indices: Vec<arena::Handle<()>>,
		ext_unix: ext::unix::Entry,
		ext_mtime: ext::mtime::Entry,
	},
	/// The object is a directory.
	Dir {
		children: FxHashMap<&'a Name, State<'a>>,
		indices: Vec<arena::Handle<()>>,
		ext_unix: ext::unix::Entry,
		ext_mtime: ext::mtime::Entry,
	},
}

impl<'a> State<'a> {
	fn file_mut(&mut self) -> &mut RangeSet<u64> {
		let Self::File { contents, .. } = self else { panic!("not a file") };
		contents
	}

	fn dir_mut(&mut self) -> &mut FxHashMap<&'a Name, State<'a>> {
		let Self::Dir { children, .. } = self else { panic!("not a dir") };
		children
	}

	fn indices_mut(&mut self) -> &mut Vec<arena::Handle<()>> {
		match self {
			Self::File { indices, .. } | Self::Dir { indices, .. } => indices,
		}
	}

	fn ext_unix_mut(&mut self) -> &mut ext::unix::Entry {
		match self {
			Self::File { ext_unix, .. } | Self::Dir { ext_unix, .. } => ext_unix,
		}
	}

	fn ext_mtime_mut(&mut self) -> &mut ext::mtime::Entry {
		match self {
			Self::File { ext_mtime, .. } | Self::Dir { ext_mtime, .. } => ext_mtime,
		}
	}
}

/// Get a mutable reference to a [`State`] node.
///
/// Fails if one of the nodes could not be found or is a [`State::File`].
fn state_mut<'a, 'b, I>(mut state: &'b mut State<'a>, path: I) -> Option<&'b mut State<'a>>
where
	I: IntoIterator<Item = &'a Name>,
{
	for p in path {
		match state {
			State::File { .. } => return None,
			State::Dir { children, .. } => state = children.get_mut(&p)?,
		}
	}
	Some(state)
}

#[derive(Debug, Arbitrary)]
pub enum Op<'a> {
	/// Create a file.
	CreateFile { dir_idx: u16, name: &'a Name, ext: Extensions },
	/// Create a directory.
	CreateDir { dir_idx: u16, name: &'a Name, options: DirOptions, ext: Extensions },
	/// Get an entry.
	Get { dir_idx: u16, name: &'a Name },
	/// Get a reference to the root directory.
	Root,
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
	/// Remove an entry.
	Remove { dir_idx: u16, name: &'a Name },
	/// Set `unix` extension data.
	SetExtUnix { idx: u16, ext: ext::unix::Entry },
	/// Set `mtime` extension data.
	SetExtMtime { idx: u16, ext: ext::mtime::Entry },
	/// Get and verify extension data.
	GetExt { idx: u16 },
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

	pub fn run(self) {
		run(async {
			// References to entries.
			let mut refs = arena::Arena::<(_, Box<[&Name]>), ()>::new();
			// Expected contents of the filesystem,
			let mut state = State::Dir {
				children: Default::default(),
				indices: Default::default(),
				ext_unix: Default::default(),
				ext_mtime: Default::default(),
			};

			fn get<'a, 'b, 'c, 'd>(
				fs: &'b Nrfs<MemDev>,
				refs: &'c arena::Arena<(RawEntryRef, Box<[&'a Name]>), ()>,
				state: &'d mut State<'a>,
				file_idx: u16,
			) -> Option<(
				TmpRef<'c, Entry<'b, MemDev>>,
				&'c [&'a Name],
				&'d mut State<'a>,
			)> {
				match refs.get(arena::Handle::from_raw(file_idx.into(), ())) {
					Some((RawEntryRef::File(file), path)) => {
						let c = state_mut(state, path.iter().copied()).unwrap();
						Some((file.into_tmp(fs).into(), path, c))
					}
					Some((RawEntryRef::Dir(dir), path)) => {
						let c = state_mut(state, path.iter().copied()).unwrap();
						Some((dir.into_tmp(fs).into(), path, c))
					}
					_ => None,
				}
			}

			fn get_dir<'a, 'b, 'c, 'd>(
				fs: &'b Nrfs<MemDev>,
				refs: &'c arena::Arena<(RawEntryRef, Box<[&'a Name]>), ()>,
				state: &'d mut State<'a>,
				dir_idx: u16,
			) -> Option<(
				TmpRef<'c, DirRef<'b, MemDev>>,
				&'c [&'a Name],
				&'d mut FxHashMap<&'a Name, State<'a>>,
			)> {
				match refs.get(arena::Handle::from_raw(dir_idx.into(), ())) {
					Some((RawEntryRef::Dir(dir), path)) => {
						let d = state_mut(state, path.iter().copied()).unwrap().dir_mut();
						Some((dir.into_tmp(fs), path, d))
					}
					_ => None,
				}
			}

			fn get_file<'a, 'b, 'c, 'd>(
				fs: &'b Nrfs<MemDev>,
				refs: &'c arena::Arena<(RawEntryRef, Box<[&'a Name]>), ()>,
				state: &'d mut State<'a>,
				file_idx: u16,
			) -> Option<(
				TmpRef<'c, FileRef<'b, MemDev>>,
				&'c [&'a Name],
				&'d mut RangeSet<u64>,
			)> {
				match refs.get(arena::Handle::from_raw(file_idx.into(), ())) {
					Some((RawEntryRef::File(file), path)) => {
						let c = state_mut(state, path.iter().copied()).unwrap().file_mut();
						Some((file.into_tmp(fs), path, c))
					}
					_ => None,
				}
			}

			fn append<'a>(path: &[&'a Name], name: &'a Name) -> Box<[&'a Name]> {
				path.iter().copied().chain([name]).collect()
			}

			for op in self.ops.into_vec() {
				match op {
					Op::CreateFile { dir_idx, name, ext } => {
						let Some((dir, _, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };

						if dir.create_file(name, &ext).await.unwrap().is_some() {
							let r = d.insert(
								name,
								State::File {
									contents: Default::default(),
									indices: Default::default(),
									ext_unix: Default::default(),
									ext_mtime: Default::default(),
								},
							);
							assert!(r.is_none());
						} else {
							assert!(d.contains_key(name));
						}
					}
					Op::CreateDir { dir_idx, name, options, ext } => {
						let Some((dir, _, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };

						if dir
							.create_dir(name, &options, &ext)
							.await
							.unwrap()
							.is_some()
						{
							let r = d.insert(
								name,
								State::Dir {
									children: Default::default(),
									indices: Default::default(),
									ext_unix: Default::default(),
									ext_mtime: Default::default(),
								},
							);
							assert!(r.is_none());
						} else {
							assert!(d.contains_key(name));
						}
					}
					Op::Get { dir_idx, name } => {
						let Some((dir, path, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };

						let path = append(path, name);
						if let Some(entry) = dir.find(name).await.unwrap() {
							let state = d.get_mut(name).unwrap();
							let r = match entry {
								Entry::File(e) => RawEntryRef::File(e.into_raw()),
								Entry::Dir(e) => RawEntryRef::Dir(e.into_raw()),
								_ => panic!("unexpected entry type"),
							};
							let idx = refs.insert((r, path));
							state.indices_mut().push(idx);
						} else {
							assert!(!d.contains_key(name));
						}
					}
					Op::Root => {
						let dir = self.fs.root_dir().await.unwrap();
						let idx = refs.insert((RawEntryRef::Dir(dir.into_raw()), [].into()));
						state.indices_mut().push(idx);
					}
					Op::Drop { idx } => {
						let idx = arena::Handle::from_raw(idx.into(), ());
						if let Some((entry, path)) = refs.remove(idx) {
							// Drop reference
							match entry {
								RawEntryRef::File(e) => {
									FileRef::from_raw(&self.fs, e);
								}
								RawEntryRef::Dir(e) => {
									DirRef::from_raw(&self.fs, e);
								}
							}
							// Remove from state
							let indices = state_mut(&mut state, path.iter().copied())
								.unwrap()
								.indices_mut();
							let i = indices.iter().position(|e| e == &idx).unwrap();
							indices.swap_remove(i);
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
					}
					Op::Read { file_idx, offset, amount } => {
						let Some((file, _, contents)) = get_file(&self.fs, &refs, &mut state, file_idx) else { continue };

						// Wrap offset
						let len = file.len().await.unwrap();
						let offt = offset.checked_rem(len).unwrap_or(0);

						// Read
						let buf = &mut vec![0; amount.into()];
						let l = file.read(offt, buf).await.unwrap();

						// Verify contents
						if l > 0 {
							for (i, c) in (offt..offt + u64::try_from(l).unwrap()).zip(&*buf) {
								assert_eq!(contents.contains(&i), *c == 1);
							}
						}
					}
					Op::Resize { file_idx, len } => {
						let Some((file, _, contents)) = get_file(&self.fs, &refs, &mut state, file_idx) else { continue };
						file.resize(len).await.unwrap();
						if len < u64::MAX {
							contents.remove(len..u64::MAX);
						}
					}
					Op::Rename { dir_idx, from, to } => {
						let Some((dir, _, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };
						if dir.rename(from, to).await.unwrap() {
							// Rename succeeded
							let mut e = d.remove(from).unwrap();

							for &idx in e.indices_mut().iter() {
								let l = &refs[idx].1;
								refs[idx].1 =
									l[..l.len() - 1].iter().copied().chain([to]).collect();
							}

							assert!(d.insert(to, e).is_none());
						} else {
							// Rename failed
							assert!(!d.contains_key(from) || d.contains_key(to));
						}
					}
					Op::Transfer { from_dir_idx, from, to_dir_idx, to } => {
						let Some((to_dir, to_path, _)) = get_dir(&self.fs, &refs, &mut state, to_dir_idx) else { continue };
						let Some((from_dir, _, d)) = get_dir(&self.fs, &refs, &mut state, from_dir_idx) else { continue };

						if from_dir.transfer(from, &to_dir, to).await.unwrap() {
							// Transfer succeeded

							let mut e = d.remove(from).unwrap();
							let d = state_mut(&mut state, to_path.iter().copied()).unwrap();

							let to_path = to_path.iter().copied().chain([to]).collect::<Box<_>>();
							for &idx in e.indices_mut().iter() {
								refs[idx].1 = to_path.clone();
							}

							let r = d.dir_mut().insert(to, e);
							assert!(r.is_none());
						} else {
							// Transfer failed
							//
							// There are many possible reasons for failure, so don't bother
							// checking for the conditions yet.
						}
					}
					Op::Remove { dir_idx, name } => {
						let Some((dir, _, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };

						match dir.remove(name).await.unwrap() {
							// Remove succeeded:
							// - the entry exists and there are no active refs.
							//   - if the entry is a directory it is empty
							Ok(()) => match d.remove(name).unwrap() {
								State::File { indices, .. } => assert!(indices.is_empty()),
								State::Dir { children, indices, .. } => {
									assert!(children.is_empty());
									assert!(indices.is_empty());
								}
							},
							// Remove failed:
							// - the entry doesn't exist.
							Err(RemoveError::NotFound) => assert!(d.get(name).is_none()),
							// - the entry exists and there are active refs.
							Err(RemoveError::LiveReference) => {
								assert!(!d.get_mut(name).unwrap().indices_mut().is_empty())
							}
							// - the entry is a non-empty directory.
							Err(RemoveError::NotEmpty) => {
								let State::Dir { children, .. } = d.get(name).unwrap() else {
									panic!()
								};
								assert!(!children.is_empty());
							}
							Err(RemoveError::UnknownType) => unreachable!(),
						}
					}
					Op::SetExtUnix { idx, ext } => {
						let Some((entry, _, _)) = get(&self.fs, &refs, &mut state, idx) else { continue };
						entry.set_ext_unix(&ext).await.unwrap();
					}
					Op::SetExtMtime { idx, ext } => {
						let Some((entry, _, _)) = get(&self.fs, &refs, &mut state, idx) else { continue };
						entry.set_ext_mtime(&ext).await.unwrap();
					}
					Op::GetExt { idx } => {
						let Some((entry, _, e)) = get(&self.fs, &refs, &mut state, idx) else { continue };
						let data = entry.data().await.unwrap();
						if let Some(data) = data.ext_unix {
							let ext = e.ext_unix_mut();
							assert_eq!(ext.permissions, data.permissions);
							assert_eq!(ext.uid(), data.uid());
							assert_eq!(ext.gid(), data.gid());
						}
						if let Some(data) = data.ext_mtime {
							let ext = e.ext_mtime_mut();
							assert_eq!(ext.mtime, data.mtime);
						}
					}
				}
			}

			// Drop all refs to ensure refcounting works properly.
			refs.drain().for_each(|(_, (r, _))| match r {
				RawEntryRef::File(e) => {
					FileRef::from_raw(&self.fs, e);
				}
				RawEntryRef::Dir(e) => {
					DirRef::from_raw(&self.fs, e);
				}
			});
		})
	}
}

use Op::*;

#[test]
fn mem_forget_dirref_into_raw() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile { dir_idx: 0, name: b"".into(), ext: Default::default() },
		],
	)
	.run()
}

/// Op::Transfer did not mem::forget to_dir if from_dir wasn't found.
#[test]
fn fuzz_transfer_mem_forget() {
	Test::new(
		1 << 16,
		[
			Root,
			Transfer { from_dir_idx: 57164, from: b"".into(), to_dir_idx: 0, to: b"".into() },
			CreateFile { dir_idx: 0, name: b"".into(), ext: Default::default() },
		],
	)
	.run()
}

#[test]
fn rename_unref_nochild() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile { dir_idx: 0, name: b"".into(), ext: Default::default() },
			Rename { dir_idx: 0, from: b"".into(), to: b"a".into() },
		],
	)
	.run()
}

#[test]
fn rename_remove_stale_index() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile { dir_idx: 0, name: (&[0]).into(), ext: Default::default() },
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
			Rename { dir_idx: 0, from: (&[]).into(), to: (&[255]).into() },
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
		],
	)
	.run()
}

#[test]
fn move_child_indices() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateDir {
				dir_idx: 0,
				name: (&[]).into(),
				options: DirOptions::new(&[
					0, 0, 0, 0, 0, 0, 2, 135, 135, 135, 135, 255, 0, 255, 255, 90,
				]),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: (&[]).into() },
			CreateFile { dir_idx: 0, name: (&[255]).into(), ext: Default::default() },
			CreateFile { dir_idx: 0, name: (&[0]).into(), ext: Default::default() },
		],
	)
	.run()
}

#[test]
fn resize_move_child_indices() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateDir {
				dir_idx: 0,
				name: (&[]).into(),
				options: DirOptions::new(&[
					0, 0, 0, 0, 0, 2, 0, 0, 0, 135, 135, 255, 0, 255, 255, 90,
				]),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: (&[]).into() },
			CreateFile { dir_idx: 0, name: (&[245]).into(), ext: Default::default() },
		],
	)
	.run()
}

/// There was a division by zero when trying to read empty files.
#[test]
fn fuzz_read_empty() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: (&[]).into() },
			Read { file_idx: 1, offset: 211106232402237, amount: 16384 },
		],
	)
	.run()
}

#[test]
fn fuzz_rename_update_path() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: (&[]).into() },
			Rename { dir_idx: 0, from: (&[]).into(), to: (&[254]).into() },
			Write { file_idx: 1, offset: 0, amount: 0 },
		],
	)
	.run()
}

/// We can't allow making directories a descendant of themselves,
/// i.e. a circular reference.
#[test]
fn transfer_circular_reference() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateDir {
				dir_idx: 0,
				name: (&[]).into(),
				options: DirOptions::new(&[89, 89, 0, 225, 0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 1, 254]),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: (&[]).into() },
			Transfer { from_dir_idx: 0, from: (&[]).into(), to_dir_idx: 1, to: (&[]).into() },
		],
	)
	.run()
}

#[test]
fn rename_leaked_ref() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile {
				dir_idx: 0,
				name: (&[85, 90, 85, 85, 85, 85, 85, 85]).into(),
				ext: Default::default(),
			},
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
			CreateFile { dir_idx: 0, name: (&[93, 223]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: (&[]).into() },
			CreateFile { dir_idx: 0, name: (&[93, 131]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: (&[]).into() },
			Transfer { from_dir_idx: 0, from: (&[]).into(), to_dir_idx: 0, to: (&[]).into() },
		],
	)
	.run()
}

#[test]
fn resize_children_backshift() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
			CreateFile { dir_idx: 0, name: (&[93, 131]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: (&[93, 131]).into() },
			Get { dir_idx: 0, name: (&[]).into() },
			CreateFile { dir_idx: 0, name: (&[0]).into(), ext: Default::default() },
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
		],
	)
	.run()
}

#[test]
fn insert_child_move_misuse() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile {
				dir_idx: 0,
				name: (&[85, 90, 85, 85, 85, 85, 85, 85]).into(),
				ext: Default::default(),
			},
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
			CreateFile {
				dir_idx: 0,
				name: (&[93, 223, 43, 0, 255, 255, 255, 255]).into(),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: (&[]).into() },
			CreateFile {
				dir_idx: 0,
				name: (&[93, 131, 239, 85, 98, 255, 77, 250]).into(),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: (&[]).into() },
			CreateFile { dir_idx: 0, name: (&[0]).into(), ext: Default::default() },
		],
	)
	.run();
}

#[test]
fn file_borrow_error() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
			CreateFile { dir_idx: 0, name: (&[0]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: (&[]).into() },
			CreateFile { dir_idx: 0, name: (&[127]).into(), ext: Default::default() },
			Rename { dir_idx: 0, from: (&[0]).into(), to: (&[]).into() },
		],
	)
	.run()
}

#[test]
fn file_resize_embed_truncated() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: (&[]).into() },
			Resize { file_idx: 1, len: 60 },
			CreateFile { dir_idx: 4864, name: (&[]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: (&[]).into() },
			Resize { file_idx: 1, len: 31232 },
		],
	)
	.run()
}

/// Newly added op with bugs in the op handling :P
#[test]
fn fuzz_op_remove() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
			Remove { dir_idx: 0, name: (&[]).into() },
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
		],
	)
	.run()
}

#[test]
fn rename_fail_dangling_ref() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile { dir_idx: 0, name: (&[]).into(), ext: Default::default() },
			CreateFile {
				dir_idx: 0,
				name: (&[95, 223, 43, 65, 255, 255, 255, 255]).into(),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: (&[]).into() },
			Rename {
				dir_idx: 0,
				from: (&[]).into(),
				to: (&[95, 223, 43, 65, 255, 255, 255, 255]).into(),
			},
			Remove { dir_idx: 0, name: (&[]).into() },
		],
	)
	.run()
}

#[test]
fn set_ext_borrow_error() {
	Test::new(
		1 << 16,
		[Root, SetExtMtime { idx: 0, ext: Default::default() }],
	)
	.run()
}

#[test]
fn transfer_childless() {
	Test::new(
		1 << 16,
		// Yes, this is actually minimal.
		// ...
		// No it isn't, but idgaf
		[
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			Root,
			CreateDir {
				dir_idx: 0,
				name: (&[]).into(),
				options: DirOptions::new(&[0; 16]),
				ext: Extensions { unix: None, mtime: None },
			},
			Root,
			Root,
			Root,
			Root,
			Get { dir_idx: 0, name: (&[]).into() },
			Transfer { from_dir_idx: 0, from: (&[]).into(), to_dir_idx: 0, to: (&[65]).into() },
			CreateFile {
				dir_idx: 23,
				name: (&[]).into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Transfer { from_dir_idx: 23, from: (&[]).into(), to_dir_idx: 0, to: (&[]).into() },
		],
	)
	.run()
}

/// This one somehow fixed itself, I haven't got a clue.
#[test]
fn magically_fixed_no_dirdata_with_id() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateDir {
				dir_idx: 0,
				name: (&[]).into(),
				options: DirOptions {
					capacity: DirSize::B512,
					extensions: Default::default(),
					hasher: Hasher::SipHasher13([
						1, 0, 79, 79, 86, 79, 79, 252, 67, 58, 255, 255, 255, 255, 0, 0,
					]),
				},
				ext: Extensions { unix: None, mtime: None },
			},
			Get { dir_idx: 0, name: (&[]).into() },
			Rename { dir_idx: 0, from: (&[]).into(), to: (&[0]).into() },
			CreateDir {
				dir_idx: 0,
				name: (&[]).into(),
				options: DirOptions {
					capacity: DirSize::B1,
					extensions: *EnableExtensions::default().add_unix().add_mtime(),
					hasher: Hasher::SipHasher13([
						113, 65, 67, 149, 67, 253, 0, 0, 25, 67, 0, 0, 1, 0, 0, 0,
					]),
				},
				ext: Extensions { unix: None, mtime: None },
			},
		],
	)
	.run()
}
