use {
	super::*,
	arbitrary::{Arbitrary, Unstructured},
	core::{hash::Hasher, mem},
	rangemap::RangeSet,
	rustc_hash::{FxHashMap, FxHasher},
	std::collections::hash_map,
};

#[derive(Debug)]
pub struct Test {
	/// Filesystem to operate on.
	fs: Nrfs<MemDev>,
	/// Ops to execute
	ops: Box<[Op]>,
	/// Expected state of the filesystem,
	/// starting from the root.
	state: FxHashMap<Box<[u8]>, State>,
}

/// Expected state of an object somewhere in the filesystem tree.
#[derive(Debug)]
enum State {
	/// The object is a file.
	File { contents: RangeSet<u64> },
	/// The object is a directory.
	Dir { children: FxHashMap<Box<[u8]>, State> },
}

/// Generate random new seed from other seed.
fn next_seed(seed: u32) -> u32 {
	let mut h = FxHasher::default();
	h.write_u32(seed);
	h.finish() as _
}

/// Randomly select a file using the given seed.
///
/// Calls the given closure for every path component except the last.
async fn select_mut<'a, F, Fut>(
	mut children: &'a mut FxHashMap<Box<[u8]>, State>,
	mut seed: u32,
	f: &mut F,
) -> (&'a [u8], &'a mut RangeSet<u64>)
where
	F: FnMut(&[u8]) -> Fut,
	Fut: Future<Output = ()>,
{
	loop {
		let offt = seed as usize % children.len();
		let (path, state) = children.iter_mut().skip(offt).next().unwrap();
		match state {
			State::File { contents } => break (&path, contents),
			State::Dir { children: c } => children = c,
		}
		f(path).await;
		seed = next_seed(seed);
	}
}

#[derive(Debug, Arbitrary)]
pub enum Op {
	/// Create a file at the given path.
	///
	/// May create intermediate directories.
	/// If any of the path components conflict with an existing file,
	/// the file is destroyed.
	Create { path: Box<[Box<[u8]>]> },
	/// Write to a file.
	///
	/// If `offset + amount` overflows, it is truncated to `u64::MAX`.
	///
	/// The path followed is pseudorandom, depending on seed.
	Write { seed: u32, offset: u64, amount: u16 },
	/// Read from a file.
	///
	/// The path followed is pseudorandom, depending on seed.
	/// `offset` is modulo length of file.
	///
	/// Also verifies contents.
	Read { seed: u32, offset: u64, amount: u16 },
	/// Remount the filesystem.
	Remount,
	/// Rename a file.
	Rename { from_seed: u32, to_name: Box<[u8]> },
	/// Transfer a file.
	///
	/// This also destroys all directories that may have become empty.
	Transfer { from_seed: u32, to_path: Box<[Box<[u8]>]> },
	/// Resize a file.
	Resize { seed: u32, size: u64 },
}

impl<'a> Arbitrary<'a> for Test {
	fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
		let mut ops = Vec::new();

		while u.arbitrary_iter()

		Ok(Self::new(
			1 << 16,
			u.arbitrary_take_rest_iter::<Op>()?
				.try_collect::<Box<_>>()?,
		))
	}
}

impl Test {
	pub fn new(blocks: usize, ops: impl Into<Box<[Op]>>) -> Self {
		Self {
			fs: run(new_cap(blocks, BlockSize::B512, MaxRecordSize::B512)),
			ops: ops.into(),
			state: Default::default(),
		}
	}

	/// Create or get directories recursively.
	///
	/// Returns a reference to the last directory in the path.
	async fn create_dirs(
		&mut self,
		path: Vec<Box<[u8]>>,
	) -> (DirRef<'_, MemDev>, &mut FxHashMap<Box<[u8]>, State>) {
		let mut state = &mut self.state;
		let mut dir = self.fs.root_dir().await.unwrap();

		for p in path {
			// Create new state if necessary.
			let name = Box::<Name>::try_from(p.clone()).unwrap();
			match state.entry(p) {
				hash_map::Entry::Vacant(e) => {
					// Create new directory.
					let State::Dir { children } = e.insert(State::Dir { children: Default::default() }) else { unreachable!() };
					state = children;
					dir = dir
						.create_dir(&name, &DirOptions::new(&[0; 16]), &Default::default())
						.await
						.unwrap()
						.unwrap();
				}
				hash_map::Entry::Occupied(e) => match e.get_mut() {
					State::Dir { children } => {
						// Reuse existing directory
						state = children;
						let Entry::Dir(d) = dir.find(&name).await.unwrap().unwrap();
						dir = d;
					}
					s @ State::File { .. } => {
						// Destroy the file and replace with directory.
						let res = dir
							.find(&name)
							.await
							.unwrap()
							.unwrap()
							.destroy()
							.await
							.unwrap();
						assert!(res, "failed to destroy file");
						*s = State::Dir { children: Default::default() };
						let State::Dir { children } = s else { unreachable!() };
						state = children;
						dir = dir
							.create_dir(&name, &DirOptions::new(&[0; 16]), &Default::default())
							.await
							.unwrap()
							.unwrap();
					}
				},
			}
		}

		(dir, state)
	}

	pub fn run(mut self) {
		run(async {
			for op in self.ops.into_vec() {
				match op {
					Op::Create { path } => {
						// Create intermediate directories.
						let mut path = path.into_vec();
						let file = path.pop().unwrap();
						let (dir, state) = self.create_dirs(path).await;

						// Create file
						let name = Box::<Name>::try_from(file).unwrap();
						if let Some(f) =
							state.insert(file, State::File { contents: Default::default() })
						{
							// Destroy whatever came before.
							dir.find(&name)
								.await
								.unwrap()
								.unwrap()
								.destroy()
								.await
								.unwrap();
						}
						dir.create_file(&name, &Default::default())
							.await
							.unwrap()
							.unwrap();
					}
					Op::Write { seed, offset, amount } => {
						// Find parent dir
						let mut dir = self.fs.root_dir().await.unwrap();
						let (path, contents) = select_mut(&mut self.state, seed, &mut |path| async move {
							let Entry::Dir(d) = dir.find(path.try_into().unwrap()).await.unwrap().unwrap() else { panic!("expected dir") };
							dir = d;
						}).await;

						// Find file
						let Entry::File(file) = dir.find(path.try_into().unwrap()).await.unwrap().unwrap() else { panic!("expected file") };

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
					Op::Read { seed, offset, amount } => {
						// Find parent dir
						let mut dir = self.fs.root_dir().await.unwrap();
						let (path, contents) = select_mut(&mut self.state, seed, &mut |path| async move {
							let Entry::Dir(d) = dir.find(path.try_into().unwrap()).await.unwrap().unwrap() else { panic!("expected dir") };
							dir = d;
						}).await;

						// Find file
						let Entry::File(file) = dir.find(path.try_into().unwrap()).await.unwrap().unwrap() else { panic!("expected file") };

						// Wrap offset
						let len = file.len().await.unwrap();
						let offt = offset % len;

						// Read
						let buf = &mut vec![0; amount.into()];
						let l = file.read(offt, &mut buf).await.unwrap();

						// Verify contents
						if l > 0 {
							for (i, c) in (offt..offt + u64::try_from(l).unwrap()).zip(&*buf) {
								assert_eq!(contents.contains(&i), *c == 1);
							}
						}
					}
					Op::Remount => {
						let devs = self.fs.unmount().await.unwrap();
						self.fs = Nrfs::load(devs, 4096, 4096).await.unwrap();
					}
					Op::Transfer { from_seed, to_path } => {
						// Find 'from' parent dir
						let mut from_dir = self.fs.root_dir().await.unwrap();
						let (from_path, contents) = select_mut(&mut self.state, from_seed, &mut |path| async move {
							let Entry::Dir(d) = dir.find(path.try_into().unwrap()).await.unwrap().unwrap() else { panic!("expected dir") };
							dir = d;
						}).await;

						// Create 'to' parent dirs
						let mut to_path = to_path.into_vec();
						let to_file = to_path.pop().unwrap();
						let (to_dir, to_state) = self.create_dirs(to_path).await;

						// Transfer
						let success = from_dir
							.transfer(
								from_path.try_into().unwrap(),
								&to_dir,
								(&*to_file).try_into().unwrap()(),
							)
							.await
							.unwrap();
						assert!(success);

						// Transfer contents
						let r =
							to_state.insert(to_file, State::File { contents: mem::take(contents) });
						assert!(r.is_none(), "file already existed");

						// Destroy empty 'from' dirs

						let c = self.contents.remove(&from_id).unwrap();
						self.contents.insert(to_id, c);
					}
					Op::Resize { idx, size } => {
						let id = self.ids[idx as usize % self.ids.len()];
						let obj = self.store.get(id).await.unwrap();
						obj.resize(size).await.unwrap();
						if size < u64::MAX {
							self.contents.get_mut(&id).unwrap().remove(size..u64::MAX);
						}
					}
				}
			}
		})
	}
}

use Op::*;
