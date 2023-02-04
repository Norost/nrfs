use {
	super::*,
	crate::dir::{Hasher, InsertError, RemoveError, RenameError, TransferError},
	arbitrary::{Arbitrary, Unstructured},
	rangemap::RangeSet,
	rustc_hash::FxHashMap,
};

#[derive(Debug)]
enum RawItemRef {
	File {
		/// Reference to the file.
		file: RawFileRef,
		/// Whether the corresponding entry was removed from the directory,
		/// i.e. whether the corresponding item is dangling.
		removed: bool,
	},
	Dir {
		/// Reference to the directory.
		dir: RawDirRef,
		/// Whether the corresponding entry was removed from the directory,
		/// i.e. whether the corresponding item is dangling.
		removed: bool,
	},
}

type Refs<'a> = arena::Arena<(RawItemRef, Box<[&'a Name]>), ()>;

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
	Dir(Dir<'a>),
}

#[derive(Debug)]
struct Dir<'a> {
	children: FxHashMap<&'a Name, State<'a>>,
	indices: Vec<arena::Handle<()>>,
	ext_unix: ext::unix::Entry,
	ext_mtime: ext::mtime::Entry,
	enabled_extensions: EnableExtensions,
}

impl<'a> State<'a> {
	fn file_mut(&mut self) -> &mut RangeSet<u64> {
		let Self::File { contents, .. } = self else { panic!("not a file") };
		contents
	}

	fn dir_mut(&mut self) -> &mut Dir<'a> {
		let Self::Dir(dir) = self else { panic!("not a dir") };
		dir
	}

	fn indices_mut(&mut self) -> &mut Vec<arena::Handle<()>> {
		match self {
			Self::File { indices, .. } | Self::Dir(Dir { indices, .. }) => indices,
		}
	}

	fn ext_unix_mut(&mut self) -> &mut ext::unix::Entry {
		match self {
			Self::File { ext_unix, .. } | Self::Dir(Dir { ext_unix, .. }) => ext_unix,
		}
	}

	fn ext_mtime_mut(&mut self) -> &mut ext::mtime::Entry {
		match self {
			Self::File { ext_mtime, .. } | Self::Dir(Dir { ext_mtime, .. }) => ext_mtime,
		}
	}

	/// Change the path of this node and descendants, if any.
	fn transfer(&mut self, refs: &mut Refs<'a>, path: &[&'a Name]) {
		fn rec<'a>(state: &mut State<'a>, refs: &mut Refs<'a>, path: &[&'a Name], depth: usize) {
			for idx in state.indices_mut() {
				let l = &refs[*idx].1;
				refs[*idx].1 = path.iter().chain(&l[l.len() - depth..]).copied().collect();
			}
			let State::Dir(Dir { children, .. }) = state else { return };
			for (_, child) in children {
				rec(child, refs, path, depth + 1);
			}
		}
		rec(self, refs, path, 0)
	}
}

impl<'a> Dir<'a> {
	fn verify_state(&self, dir: &DirRef<'_, impl Dev>) {
		let ext = dir.enabled_extensions();
		assert_eq!(self.enabled_extensions.mtime(), ext.mtime());
		assert_eq!(self.enabled_extensions.unix(), ext.unix());
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
			State::Dir(Dir { children, .. }) => state = children.get_mut(&p)?,
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
		Self { fs: new_cap(blocks, BlockSize::B512, MaxRecordSize::B512), ops: ops.into() }
	}

	pub fn run(self) {
		run(&self.fs, async {
			// References to entries.
			let mut refs = arena::Arena::<(_, Box<[&Name]>), ()>::new();
			// Expected contents of the filesystem,
			let mut state = State::Dir(Dir {
				children: Default::default(),
				indices: Default::default(),
				ext_unix: Default::default(),
				ext_mtime: Default::default(),
				enabled_extensions: Default::default(),
			});

			fn get<'a, 'b, 'c, 'd>(
				fs: &'b Nrfs<MemDev>,
				refs: &'c Refs<'a>,
				state: &'d mut State<'a>,
				idx: u16,
			) -> Option<(
				TmpRef<'c, ItemRef<'b, MemDev>>,
				&'c [&'a Name],
				Option<&'d mut State<'a>>,
			)> {
				match refs.get(arena::Handle::from_raw(idx.into(), ())) {
					Some((RawItemRef::File { file, removed }, path)) => {
						let c = (!removed).then(|| state_mut(state, path.iter().copied()).unwrap());
						Some((file.into_tmp(fs).into(), path, c))
					}
					Some((RawItemRef::Dir { dir, removed }, path)) => {
						let c = (!removed).then(|| state_mut(state, path.iter().copied()).unwrap());
						Some((dir.into_tmp(fs).into(), path, c))
					}
					_ => None,
				}
			}

			fn get_dir<'a, 'b, 'c, 'd>(
				fs: &'b Nrfs<MemDev>,
				refs: &'c Refs<'a>,
				state: &'d mut State<'a>,
				dir_idx: u16,
			) -> Option<(
				TmpRef<'c, DirRef<'b, MemDev>>,
				&'c [&'a Name],
				Option<&'d mut Dir<'a>>,
			)> {
				match refs.get(arena::Handle::from_raw(dir_idx.into(), ())) {
					Some((RawItemRef::Dir { dir, removed }, path)) => {
						let d = (!removed)
							.then(|| state_mut(state, path.iter().copied()).unwrap().dir_mut());
						Some((dir.into_tmp(fs), path, d))
					}
					_ => None,
				}
			}

			fn get_file<'a, 'b, 'c, 'd>(
				fs: &'b Nrfs<MemDev>,
				refs: &'c Refs<'a>,
				state: &'d mut State<'a>,
				file_idx: u16,
			) -> Option<(
				TmpRef<'c, FileRef<'b, MemDev>>,
				&'c [&'a Name],
				Option<&'d mut RangeSet<u64>>,
			)> {
				match refs.get(arena::Handle::from_raw(file_idx.into(), ())) {
					Some((RawItemRef::File { file, removed }, path)) => {
						let c = (!removed)
							.then(|| state_mut(state, path.iter().copied()).unwrap().file_mut());
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
					Op::CreateFile { dir_idx, name, mut ext } => {
						let Some((dir, _, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };
						d.as_ref().map(|d| d.verify_state(&dir));
						ext.mask(dir.enabled_extensions());

						match dir.create_file(name, &ext).await.unwrap() {
							Ok(file) => {
								file.drop().await.unwrap();
								let d = d.expect("dir should be empty");
								let d = &mut d.children;
								let r = d.insert(
									name,
									State::File {
										contents: Default::default(),
										indices: Default::default(),
										ext_unix: ext.unix.unwrap_or_default(),
										ext_mtime: ext.mtime.unwrap_or_default(),
									},
								);
								assert!(r.is_none());
							}
							Err(InsertError::Duplicate) => {
								let d = d.expect("dir should be empty");
								let d = &mut d.children;
								assert!(d.contains_key(name));
							}
							Err(InsertError::Full) => {
								let _ = d.expect("dir should be empty");
							}
							Err(InsertError::Dangling) => {
								// TODO find a nice way to detect false dangling status while avoiding false positives.
								// See fuzz_dangling_present_false_positive
								/*
								assert!(d.is_none(), "dir is present in state");
								*/
							}
						}
					}
					Op::CreateDir { dir_idx, name, options, mut ext } => {
						let Some((dir, _, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };
						d.as_ref().map(|d| d.verify_state(&dir));
						ext.mask(dir.enabled_extensions());

						match dir.create_dir(name, &options, &ext).await.unwrap() {
							Ok(dir) => {
								dir.drop().await.unwrap();
								let d = d.expect("dir should be empty");
								let d = &mut d.children;
								let r = d.insert(
									name,
									State::Dir(Dir {
										children: Default::default(),
										indices: Default::default(),
										ext_unix: ext.unix.unwrap_or_default(),
										ext_mtime: ext.mtime.unwrap_or_default(),
										enabled_extensions: options.extensions,
									}),
								);
								assert!(r.is_none());
							}
							Err(InsertError::Duplicate) => {
								let d = d.expect("dir should be empty");
								let d = &mut d.children;
								assert!(d.contains_key(name));
							}
							Err(InsertError::Full) => {
								let _ = d.expect("dir should be empty");
							}
							Err(InsertError::Dangling) => {
								// TODO find a nice way to detect false dangling status while avoiding false positives.
								// See fuzz_dangling_present_false_positive
								/*
								assert!(d.is_none(), "dir is present in state");
								*/
							}
						}
					}
					Op::Get { dir_idx, name } => {
						let Some((dir, path, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };

						// If the dir has been removed it should be empty.
						let d = d.map(|d| &mut d.children);
						let d_stub = &mut Default::default();
						let d = d.unwrap_or(d_stub);

						let path = append(path, name);
						if let Some(entry) = dir.find(name).await.unwrap() {
							let state = d.get_mut(name).unwrap();
							let r = match entry {
								ItemRef::File(e) => {
									RawItemRef::File { file: e.into_raw(), removed: false }
								}
								ItemRef::Dir(e) => {
									RawItemRef::Dir { dir: e.into_raw(), removed: false }
								}
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
						let idx = refs.insert((
							RawItemRef::Dir { dir: dir.into_raw(), removed: false },
							[].into(),
						));
						state.indices_mut().push(idx);
					}
					Op::Drop { idx } => {
						let idx = arena::Handle::from_raw(idx.into(), ());
						if let Some((entry, path)) = refs.remove(idx) {
							// Drop reference
							let removed = match entry {
								RawItemRef::File { file, removed } => {
									FileRef::from_raw(&self.fs, file).drop().await.unwrap();
									removed
								}
								RawItemRef::Dir { dir, removed } => {
									DirRef::from_raw(&self.fs, dir).drop().await.unwrap();
									removed
								}
							};
							if !removed {
								// Remove from state
								let indices = state_mut(&mut state, path.iter().copied())
									.unwrap()
									.indices_mut();
								let i = indices.iter().position(|e| e == &idx).unwrap();
								indices.swap_remove(i);
							}
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
							let Some(contents) = contents else {
								// TODO keep track of contents even if removed.
								continue
							};
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
							let Some(contents) = contents else {
								// TODO keep track of contents even if removed.
								continue
							};
							for (i, c) in (offt..offt + u64::try_from(l).unwrap()).zip(&*buf) {
								assert_eq!(u8::from(contents.contains(&i)), *c);
							}
						}
					}
					Op::Resize { file_idx, len } => {
						let Some((file, _, contents)) = get_file(&self.fs, &refs, &mut state, file_idx) else { continue };
						file.resize(len).await.unwrap();
						if len < u64::MAX {
							let Some(contents) = contents else {
								// TODO keep track of contents even if removed.
								continue
							};
							contents.remove(len..u64::MAX);
						}
					}
					Op::Rename { dir_idx, from, to } => {
						let Some((dir, dir_path, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };
						let Some(d) = d else {
							// Entry was already removed.
							continue
						};
						let d = &mut d.children;
						match dir.rename(from, to).await.unwrap() {
							Ok(()) => {
								// Rename succeeded
								let mut e = d.remove(from).unwrap();
								let path =
									dir_path.iter().chain(&[to]).copied().collect::<Vec<_>>();
								e.transfer(&mut refs, &path);
								assert!(d.insert(to, e).is_none());
							}
							Err(RenameError::NotFound) => {
								assert!(!d.contains_key(from));
							}
							Err(RenameError::Duplicate) => {
								assert!(d.contains_key(to));
							}
						}
					}
					Op::Transfer { from_dir_idx, from, to_dir_idx, to } => {
						let Some((to_dir, to_path, _)) = get_dir(&self.fs, &refs, &mut state, to_dir_idx) else { continue };
						let Some((from_dir, _, d)) = get_dir(&self.fs, &refs, &mut state, from_dir_idx) else { continue };

						let Some(d) = d else {
							// Entry was already removed.
							continue
						};
						let d = &mut d.children;

						match from_dir.transfer(from, &to_dir, to).await.unwrap() {
							Ok(()) => {
								// Transfer succeeded
								let mut e = d.remove(from).unwrap();
								let d = state_mut(&mut state, to_path.iter().copied()).unwrap();

								let path = to_path.iter().chain(&[to]).copied().collect::<Vec<_>>();
								e.transfer(&mut refs, &path);

								let d = d.dir_mut();
								assert!(d.children.insert(to, e).is_none());

								// Apply mask to item extensions.
								let child = d.children.get_mut(&to).unwrap();
								let ext = d.enabled_extensions;
								(!ext.unix()).then(|| *child.ext_unix_mut() = Default::default());
								(!ext.mtime()).then(|| *child.ext_mtime_mut() = Default::default());
							}
							Err(TransferError::NotFound) => {
								assert!(!d.contains_key(from));
							}
							Err(TransferError::IsAncestor) => {
								// TODO
							}
							Err(TransferError::Full) => {
								// TODO
							}
							Err(TransferError::UnknownType) => unreachable!(),
							Err(TransferError::Duplicate) => {
								// TODO
							}
							Err(TransferError::Dangling) => {
								// TODO find a nice way to detect false dangling status while avoiding false positives.
								// See fuzz_dangling_present_false_positive
								/*
								let d = state_mut(&mut state, to_path.iter().copied());
								assert!(d.is_none(), "target directory is present in state");
								*/
							}
						}
					}
					Op::Remove { dir_idx, name } => {
						let Some((dir, _, d)) = get_dir(&self.fs, &refs, &mut state, dir_idx) else { continue };

						let Some(d) = d else {
							// Entry was already removed.
							continue
						};
						let d = &mut d.children;

						match dir.remove(name).await.unwrap() {
							// Remove succeeded:
							// - if the entry is a directory it is empty
							Ok(()) => {
								let indices = match d.remove(name).unwrap() {
									State::File { indices, .. } => indices,
									State::Dir(Dir { children, indices, .. }) => {
										assert!(children.is_empty());
										indices
									}
								};
								for &i in indices.iter() {
									match &mut refs[i].0 {
										RawItemRef::File { removed, .. } => *removed = true,
										RawItemRef::Dir { removed, .. } => *removed = true,
									}
								}
							}
							// Remove failed:
							// - the entry doesn't exist.
							Err(RemoveError::NotFound) => assert!(d.get(name).is_none()),
							// - the entry is a non-empty directory.
							Err(RemoveError::NotEmpty) => {
								let State::Dir(Dir { children, .. }) = d.get(name).unwrap() else {
									panic!()
								};
								assert!(!children.is_empty());
							}
							Err(RemoveError::UnknownType) => unreachable!(),
						}
					}
					Op::SetExtUnix { idx, ext } => {
						let Some((entry, _, e)) = get(&self.fs, &refs, &mut state, idx) else { continue };
						entry.set_ext_unix(&ext).await.unwrap();
						// TODO keep track of dangling objects.
						e.map(|e| *e.ext_unix_mut() = ext);
					}
					Op::SetExtMtime { idx, ext } => {
						let Some((entry, _, e)) = get(&self.fs, &refs, &mut state, idx) else { continue };
						entry.set_ext_mtime(&ext).await.unwrap();
						// TODO keep track of dangling objects.
						e.map(|e| *e.ext_mtime_mut() = ext);
					}
					Op::GetExt { idx } => {
						let Some((entry, _, e)) = get(&self.fs, &refs, &mut state, idx) else { continue };
						let data = entry.data().await.unwrap();
						if let Some(e) = e {
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
						} else {
							// TODO keep tracking metadata even if removed.
						}
					}
				}
			}

			// Drop all refs to ensure refcounting works properly.
			for (_, (r, _)) in refs.drain() {
				match r {
					RawItemRef::File { file, .. } => {
						FileRef::from_raw(&self.fs, file).drop().await.unwrap()
					}
					RawItemRef::Dir { dir, .. } => {
						DirRef::from_raw(&self.fs, dir).drop().await.unwrap()
					}
				}
			}
		});
	}
}

use Op::*;

#[test]
fn mem_forget_dirref_into_raw() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
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
			Transfer { from_dir_idx: 57164, from: b"x".into(), to_dir_idx: 0, to: b"x".into() },
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
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
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
			Rename { dir_idx: 0, from: b"x".into(), to: b"a".into() },
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
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
			Rename { dir_idx: 0, from: b"x".into(), to: (&[255]).into() },
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
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
				name: b"x".into(),
				options: DirOptions::new(&[
					0, 0, 0, 0, 0, 0, 2, 135, 135, 135, 135, 255, 0, 255, 255, 90,
				]),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: b"x".into() },
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
				name: b"x".into(),
				options: DirOptions::new(&[
					0, 0, 0, 0, 0, 2, 0, 0, 0, 135, 135, 255, 0, 255, 255, 90,
				]),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: b"x".into() },
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
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
			Get { dir_idx: 0, name: b"x".into() },
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
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
			Get { dir_idx: 0, name: b"x".into() },
			Rename { dir_idx: 0, from: b"x".into(), to: (&[254]).into() },
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
				name: b"x".into(),
				options: DirOptions::new(&[89, 89, 0, 225, 0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 1, 254]),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: b"x".into() },
			Transfer { from_dir_idx: 0, from: b"x".into(), to_dir_idx: 1, to: b"x".into() },
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
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
			CreateFile { dir_idx: 0, name: (&[93, 223]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: b"x".into() },
			CreateFile { dir_idx: 0, name: (&[93, 131]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: b"x".into() },
			Transfer { from_dir_idx: 0, from: b"x".into(), to_dir_idx: 0, to: b"x".into() },
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
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
			CreateFile { dir_idx: 0, name: (&[93, 131]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: (&[93, 131]).into() },
			Get { dir_idx: 0, name: b"x".into() },
			CreateFile { dir_idx: 0, name: (&[0]).into(), ext: Default::default() },
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
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
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
			CreateFile {
				dir_idx: 0,
				name: (&[93, 223, 43, 0, 255, 255, 255, 255]).into(),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: b"x".into() },
			CreateFile {
				dir_idx: 0,
				name: (&[93, 131, 239, 85, 98, 255, 77, 250]).into(),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: b"x".into() },
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
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
			CreateFile { dir_idx: 0, name: (&[0]).into(), ext: Default::default() },
			Get { dir_idx: 0, name: b"x".into() },
			CreateFile { dir_idx: 0, name: (&[127]).into(), ext: Default::default() },
			Rename { dir_idx: 0, from: (&[0]).into(), to: b"x".into() },
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
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
			Get { dir_idx: 0, name: b"x".into() },
			Resize { file_idx: 1, len: 60 },
			CreateFile { dir_idx: 4864, name: b"x".into(), ext: Default::default() },
			Get { dir_idx: 0, name: b"x".into() },
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
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
			Remove { dir_idx: 0, name: b"x".into() },
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
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
			CreateFile { dir_idx: 0, name: b"x".into(), ext: Default::default() },
			CreateFile {
				dir_idx: 0,
				name: (&[95, 223, 43, 65, 255, 255, 255, 255]).into(),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: b"x".into() },
			Rename {
				dir_idx: 0,
				from: b"x".into(),
				to: (&[95, 223, 43, 65, 255, 255, 255, 255]).into(),
			},
			Remove { dir_idx: 0, name: b"x".into() },
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
				name: b"x".into(),
				options: DirOptions::new(&[0; 16]),
				ext: Extensions { unix: None, mtime: None },
			},
			Root,
			Root,
			Root,
			Root,
			Get { dir_idx: 0, name: b"x".into() },
			Transfer { from_dir_idx: 0, from: b"x".into(), to_dir_idx: 0, to: (&[65]).into() },
			CreateFile {
				dir_idx: 23,
				name: b"x".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Transfer { from_dir_idx: 23, from: b"x".into(), to_dir_idx: 0, to: b"x".into() },
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
				name: b"x".into(),
				options: DirOptions {
					extensions: Default::default(),
					hasher: Hasher::SipHasher13([
						1, 0, 79, 79, 86, 79, 79, 252, 67, 58, 255, 255, 255, 255, 0, 0,
					]),
				},
				ext: Extensions { unix: None, mtime: None },
			},
			Get { dir_idx: 0, name: b"x".into() },
			Rename { dir_idx: 0, from: b"x".into(), to: (&[0]).into() },
			CreateDir {
				dir_idx: 0,
				name: b"x".into(),
				options: DirOptions {
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

#[test]
fn insert_child_unmoved() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile {
				dir_idx: 0,
				name: b"x".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Get { dir_idx: 0, name: b"x".into() },
			CreateFile {
				dir_idx: 0,
				name: b"5\xB9\0AAC\x13AA\xFF\xFF\xFF\xFF\xFF\xFF\0\0\0\0".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			CreateFile {
				dir_idx: 0,
				name: b"\x17\x17\x17\x17\x17\0\0\x17KJOOA\xFF\xFFAAAAA\0AA".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			CreateFile {
				dir_idx: 0,
				name: b"\xFF\x0f\0\x80\0/\0\x8D\x8D\x05\x07\0OOQ\x13".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Remove { dir_idx: 0, name: b"x".into() },
		],
	)
	.run()
}

/// Also magically fixed...
#[test]
fn magically_fixed_resize_child_move() {
	Test::new(
		1 << 16,
		[
			Root,
			Get { dir_idx: 0, name: b"x".into() },
			CreateFile {
				dir_idx: 0,
				name: b"AAA\xFD".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Rename { dir_idx: 0, from: b"x".into(), to: b"x".into() },
		],
	)
	.run()
}

/// Don't forget to apply the mask to indices!
#[test]
fn remove_next_index_mask() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile {
				dir_idx: 0,
				name: b"x".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Get { dir_idx: 0, name: b"x".into() },
			CreateFile {
				dir_idx: 0,
				name: b"\0C".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			CreateFile {
				dir_idx: 0,
				name: b"\xFF\0\0\0\0\0\0\0".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Transfer { from_dir_idx: 0, from: b"x".into(), to_dir_idx: 0, to: b"\0\0\x19C".into() },
			CreateFile {
				dir_idx: 0,
				name: b"x".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			CreateFile {
				dir_idx: 0,
				name: b"\xFF\0\0Q\x08\0 \0".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Transfer { from_dir_idx: 0, from: b"x".into(), to_dir_idx: 0, to: b"x".into() },
		],
	)
	.run()
}

/// Out-of-bounds error with `Test::run::get`.
///
/// This is a new error since items with live references can now be "removed".
#[test]
fn fuzz_access_removed_ref() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile {
				dir_idx: 0,
				name: b"\0".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Get { dir_idx: 0, name: b"\0".into() },
			Remove { dir_idx: 0, name: b"\0".into() },
			GetExt { idx: 1 },
		],
	)
	.run()
}

/// Directories that have been removed may never accept new entries.
#[test]
fn forbid_insert_removed_dir() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateDir {
				dir_idx: 0,
				name: b"\0".into(),
				options: DirOptions::new(&[0; 16]),
				ext: Default::default(),
			},
			Root,
			Get { dir_idx: 0, name: b"\0".into() },
			Remove { dir_idx: 0, name: b"\0".into() },
			CreateFile { dir_idx: 2, name: b"?".into(), ext: Default::default() },
		],
	)
	.run()
}

/// The fuzzer falsely detected dangling objects as being present if a new object
/// with the same name was made.
#[test]
fn fuzz_dangling_present_false_positive() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateDir {
				dir_idx: 0,
				name: b"\0".into(),
				options: DirOptions::new(&[0; 16]),
				ext: Extensions::default(),
			},
			Get { dir_idx: 0, name: b"\0".into() },
			Remove { dir_idx: 0, name: b"\0".into() },
			CreateFile {
				dir_idx: 0,
				name: b"\0".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Transfer { from_dir_idx: 0, from: b"\0".into(), to_dir_idx: 1, to: b"\0".into() },
		],
	)
	.run()
}

/// Heap deallocations did not zero out memory, which can result in use-after-frees.
#[test]
fn unzeroed_deallocation() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile {
				dir_idx: 0,
				name: b"\0".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Get { dir_idx: 0, name: b"\0".into() },
			Write { file_idx: 1, offset: 245, amount: 768 },
			Resize { file_idx: 1, len: 0 },
			Resize { file_idx: 1, len: 255 },
			Read { file_idx: 1, offset: 8298290729805977344, amount: 41 },
		],
	)
	.run()
}

#[test]
fn unzeroed_deallocation_long_name() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateFile {
				dir_idx: 0,
				name: (&[b'x'; 31]).try_into().unwrap(),
				ext: Default::default(),
			},
			Get { dir_idx: 0, name: (&[b'x'; 31]).try_into().unwrap() },
			Write { file_idx: 1, offset: 245, amount: 1 },
			Resize { file_idx: 1, len: 0 },
			Resize { file_idx: 1, len: 255 },
			Read { file_idx: 1, offset: 18409113286005899008, amount: 255 },
		],
	)
	.run()
}

/// The fuzzer didn't edit the paths of descendants when renaming a directory.
#[test]
fn fuzz_rename_dir_edit_descendant_paths() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateDir {
				dir_idx: 0,
				name: b"\0".into(),
				options: DirOptions::new(&[0; 16]),
				ext: Extensions { unix: None, mtime: None },
			},
			Get { dir_idx: 0, name: b"\0".into() },
			CreateFile {
				dir_idx: 1,
				name: b"\0".into(),
				ext: Extensions { unix: None, mtime: None },
			},
			Get { dir_idx: 1, name: b"\0".into() },
			Rename { dir_idx: 0, from: b"\0".into(), to: b"\xFF".into() },
			GetExt { idx: 2 },
		],
	)
	.run()
}

/// The fuzzer wrongly saved non-default extension data in directories
/// with no extensions enabled.
#[test]
fn fuzz_ext_in_noext_dir_0() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateDir {
				dir_idx: 0,
				name: b"\0".into(),
				options: DirOptions {
					extensions: *EnableExtensions::default().add_mtime().add_unix(),
					hasher: Hasher::SipHasher13([0; 16]),
				},
				ext: Extensions { unix: None, mtime: Some(ext::mtime::Entry { mtime: 1 }) },
			},
			Get { dir_idx: 0, name: b"\x00".into() },
			Rename { dir_idx: 0, from: b"\x00".into(), to: b"\x07".into() },
			CreateFile {
				dir_idx: 0,
				name: b"\0".into(),
				ext: Extensions { unix: None, mtime: Some(ext::mtime::Entry { mtime: 2 }) },
			},
			Get { dir_idx: 0, name: b"\0".into() },
			Transfer { from_dir_idx: 0, from: b"\0".into(), to_dir_idx: 1, to: b"<".into() },
			GetExt { idx: 2 },
		],
	)
	.run()
}

#[test]
fn fuzz_ext_in_noext_dir_1() {
	Test::new(
		1 << 16,
		[
			Root,
			CreateDir {
				dir_idx: 0,
				name: b"\xff".into(),
				options: DirOptions {
					extensions: *EnableExtensions::default().add_unix().add_mtime(),
					hasher: Hasher::SipHasher13([0; 16]),
				},
				ext: Extensions { unix: None, mtime: None },
			},
			CreateDir {
				dir_idx: 0,
				name: b"\0".into(),
				options: DirOptions {
					extensions: *EnableExtensions::default().add_unix().add_mtime(),
					hasher: Hasher::SipHasher13([0; 16]),
				},
				ext: Extensions {
					unix: Some(ext::unix::Entry::new(11261, 0x10000, 0xff40)),
					mtime: None,
				},
			},
			Get { dir_idx: 0, name: b"\xff".into() },
			Get { dir_idx: 0, name: b"\0".into() },
			Transfer { from_dir_idx: 0, from: b"\0".into(), to_dir_idx: 1, to: b"\0".into() },
			GetExt { idx: 2 },
		],
	)
	.run()
}
