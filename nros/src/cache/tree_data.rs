use {
	super::{Cache, CacheData, Dev, Entry, Error, MaxRecordSize, OBJECT_LIST_ID, RECORD_SIZE_P2},
	crate::{resource::Buf, Record, Resource},
	core::{
		cell::RefCell,
		fmt,
		future::{self, Future},
		mem,
		pin::Pin,
		task::{Poll, Waker},
	},
	rustc_hash::FxHashMap,
};

/// A single cached record tree.
#[derive(Debug)]
pub struct TreeData<R: Resource> {
	/// Cached records.
	///
	/// The index in the array is correlated with depth.
	/// The key is correlated with offset.
	pub(super) data: Box<[Level<R>]>,
	/// Amount of active operations on this tree.
	///
	/// - If `0`, no operations are occuring right now.
	/// - If `-1`, a root_replace operation is in progress.
	/// - Otherwise, any number of reads or writes are in progress.
	///
	/// If the high bit is set, a root_replace operation is pending and no
	/// more reads & writes should be queued.
	active_ops: isize,
	/// Tasks waiting operate on this tree.
	wakers: Vec<Waker>,
}

impl<R: Resource> TreeData<R> {
	/// Add a new entry.
	///
	/// # Panics
	///
	/// If the entry is already present.
	pub fn add_entry(
		&mut self,
		max_record_size: MaxRecordSize,
		depth: u8,
		offset: u64,
		entry: Entry<R>,
	) {
		// Insert entry
		let _r = self.data[usize::from(depth)].entries.insert(offset, entry);
		debug_assert!(_r.is_none(), "entry was already present");

		// If dirty, propagate dirty counters
		let mut offt = offset;
		for lvl in self.data.iter_mut().skip(depth.into()) {
			*lvl.dirty_counters.entry(offt).or_insert(0) += 1;
			offt >>= max_record_size.to_raw() - RECORD_SIZE_P2;
		}
	}
}

pub struct Level<R: Resource> {
	pub(super) entries: FxHashMap<u64, Entry<R>>,
	pub(super) dirty_counters: FxHashMap<u64, usize>,
}

impl<R: Resource> Default for Level<R> {
	fn default() -> Self {
		Self { entries: Default::default(), dirty_counters: Default::default() }
	}
}

impl<R: Resource> fmt::Debug for Level<R> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Level))
			.field("entries", &self.entries)
			.field("dirty_counters", &self.dirty_counters)
			.finish()
	}
}

impl<R: Resource> TreeData<R> {
	pub fn new(depth: u8) -> Self {
		Self {
			data: (0..depth).map(|_| Default::default()).collect(),
			active_ops: 0,
			wakers: Default::default(),
		}
	}

	/// Whether this [`TreeData`] is empty,
	/// i.e. there are no cached entries and no active operations.
	pub fn is_empty(&self) -> bool {
		self.active_ops == 0 && self.data.iter().all(|m| m.entries.is_empty())
	}

	/// Fix depth of [`TreeData`] if a false depth of 0 was set.
	fn fix_depth(&mut self, max_record_size: MaxRecordSize, root: &Record) {
		if self.data.is_empty() {
			let depth = super::tree::depth(max_record_size, root.total_length.into());
			self.data = (0..depth).map(|_| Default::default()).collect();
		}
	}
}

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Acquire a lock on a tree for a read or write operation.
	///
	/// Returns the `root` of the tree as of locking.
	pub(super) async fn lock_readwrite(
		&self,
		id: u64,
	) -> Result<(ReadWriteGuard<'_, R>, Record), Error<D>> {
		trace!("lock_readwrite {}", id);
		// Acquire lock.
		let lock = future::poll_fn(move |cx| {
			// Use 0 as depth until we can safely read the root.
			let mut tree = self.get_object_entry_mut(id, 0);
			if tree.active_ops & isize::MIN != 0 {
				// A task is attempting to acquire a root_replace lock
				// Block until that task has finished.
				tree.wakers.push(cx.waker().clone());
				return Poll::Pending;
			}
			// Acquire a read/write lock.
			tree.active_ops += 1;
			Poll::Ready(ReadWriteGuard { data: &self.data, id })
		})
		.await;
		let root = box_fut(self.get_object_root(id));
		let root = root.await?;
		self.get_object_entry_mut(id, 0)
			.fix_depth(self.max_record_size(), &root);
		Ok((lock, root))
	}

	/// Acquire a lock on a tree for a root_replace operation.
	pub(super) async fn lock_root_replace(
		&self,
		id: u64,
	) -> Result<(RootReplaceGuard<'_, R>, Record), Error<D>> {
		trace!("lock_root_replace {}", id);
		let lock = future::poll_fn(move |cx| {
			// Use 0 as depth until we can safely read the root.
			let mut tree = self.get_object_entry_mut(id, 0);
			if tree.active_ops & isize::MAX != 0 {
				// One or more tasks already hold a lock.
				// Block until they all finish.
				tree.wakers.push(cx.waker().clone());
				// Indicate we're waiting for a root_replace lock.
				tree.active_ops |= isize::MIN;
				return Poll::Pending;
			}
			// Acquire a root_replace lock.
			tree.active_ops = -1;
			Poll::Ready(RootReplaceGuard { data: &self.data, id })
		})
		.await;
		let root = box_fut(self.get_object_root(id));
		let root = root.await?;
		self.get_object_entry_mut(id, 0)
			.fix_depth(self.max_record_size(), &root);
		Ok((lock, root))
	}
}

fn box_fut<'a, Fut: Future + 'a>(fut: Fut) -> Pin<Box<dyn Future<Output = Fut::Output> + 'a>> {
	Box::pin(fut)
}

/// Read and/or write guard on a tree.
pub struct ReadWriteGuard<'a, R: Resource> {
	data: &'a RefCell<CacheData<R>>,
	id: u64,
}

impl<R: Resource> Drop for ReadWriteGuard<'_, R> {
	fn drop(&mut self) {
		trace!("ReadWriteGuard::drop {}", self.id);
		let mut data = self.data.borrow_mut();
		let tree = data.data.get_mut(&self.id).expect("no tree");
		tree.active_ops -= 1;
		if tree.active_ops & isize::MAX == 0 {
			// No active rw ops, wake tasks trying to root_replace.
			tree.wakers.drain(..).for_each(|w| w.wake());
		}
	}
}

/// Resize guard on a tree.
pub struct RootReplaceGuard<'a, R: Resource> {
	data: &'a RefCell<CacheData<R>>,
	id: u64,
}

impl<R: Resource> Drop for RootReplaceGuard<'_, R> {
	fn drop(&mut self) {
		trace!("RootReplaceGuard::drop {}", self.id);
		let mut data = self.data.borrow_mut();
		let tree = data.data.get_mut(&self.id).expect("no tree");
		if cfg!(debug_assertions) && !std::thread::panicking() {
			assert_eq!(tree.active_ops, -1, "root_replace lock not held");
		}
		tree.active_ops = 0;
		// Wake other tasks.
		tree.wakers.drain(..).for_each(|w| w.wake());
	}
}

/// Formatter for [`TreeData`].
///
/// The output is more compact than that of `derive(Debug)`, especially for large amounts of data.
pub struct FmtTreeData<'a, R: Resource> {
	pub data: &'a TreeData<R>,
	pub id: u64,
}

impl<R: Resource + fmt::Debug> fmt::Debug for FmtTreeData<'_, R> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		struct FmtRecordList<'a>(&'a [u8]);

		impl fmt::Debug for FmtRecordList<'_> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				let mut f = f.debug_map();
				let mut i = 0;
				let mut index = 0;
				while i < self.0.len() {
					let mut rec = Record::default();
					let l = (self.0.len() - i).min(mem::size_of::<Record>());
					rec.as_mut()[..l].copy_from_slice(&self.0[i..][..l]);
					if rec != Record::default() {
						f.entry(&index, &rec);
					}
					i += l;
					index += 1;
				}
				f.finish()
			}
		}

		struct FmtRecord<'a, R: Resource>(&'a Entry<R>);

		impl<R: Resource> fmt::Debug for FmtRecord<'_, R> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				#[derive(Debug)]
				#[allow(dead_code)]
				struct T<'a> {
					data: FmtRecordList<'a>,
					global_index: super::lru::Idx,
					write_index: Option<super::lru::Idx>,
				}
				T {
					data: FmtRecordList(self.0.data.get()),
					global_index: self.0.global_index,
					write_index: self.0.write_index,
				}
				.fmt(f)
			}
		}

		struct FmtRecordMap<'a, R: Resource>(&'a FxHashMap<u64, Entry<R>>);

		impl<R: Resource> fmt::Debug for FmtRecordMap<'_, R> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				let mut f = f.debug_map();
				for (k, v) in self.0.iter() {
					f.entry(k, &FmtRecord(v));
				}
				f.finish()
			}
		}

		struct FmtRecordLevel<'a, R: Resource>(&'a Level<R>);

		impl<R: Resource> fmt::Debug for FmtRecordLevel<'_, R> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				f.debug_struct(stringify!(Level))
					.field("entries", &FmtRecordMap(&self.0.entries))
					.field("dirty_counters", &self.0.dirty_counters)
					.finish()
			}
		}

		struct FmtData<'a, R: Resource>(&'a FmtTreeData<'a, R>);

		impl<R: Resource> fmt::Debug for FmtData<'_, R> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				let mut f = f.debug_map();
				let mut depths = self.0.data.data.iter().enumerate();
				// Format like data
				if self.0.id != OBJECT_LIST_ID {
					let Some((i, l)) = depths.next() else { return f.finish() };
					f.entry(&i, l);
				}
				// Format like records
				for (i, l) in depths {
					f.entry(&i, &FmtRecordLevel(l));
				}
				f.finish()
			}
		}

		f.debug_struct(stringify!(TreeData))
			.field("data", &FmtData(self))
			.finish()
	}
}
