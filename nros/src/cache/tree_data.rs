use crate::Tree;

use {
	super::{CacheData, Entry, OBJECT_LIST_ID},
	crate::Record,
	core::{
		cell::RefCell,
		fmt,
		future::{self, Future},
		mem,
		task::{Poll, Waker},
	},
	rustc_hash::FxHashMap,
};

/// A single cached record tree.
pub struct TreeData {
	/// Cached records.
	///
	/// The index in the array is correlated with depth.
	/// The key is correlated with offset.
	pub data: Box<[FxHashMap<u64, Entry>]>,
	/// Lock on the data of this tree.
	///
	/// This is to prevent race conditions with concurrent writes, flushing ...
	lock: Lock,
	/// Wakers for tasks attempting to operate on this tree.
	wakers: Vec<Waker>,
}

impl TreeData {
	pub fn new(depth: u8) -> Self {
		Self {
			data: (0..depth).map(|_| Default::default()).collect(),
			lock: Lock::None,
			wakers: Default::default(),
		}
	}

	/// Check if a flush lock is active.
	pub fn is_flush_locked(&self) -> bool {
		matches!(self.lock, Lock::Flush)
	}
}

#[derive(Debug)]
enum Lock {
	/// No lock is active.
	None,
	/// Read lock, with amount of readers.
	///
	/// Prevents writes.
	Read { readers: u32 },
	/// Pending write lock, with amount of readers.
	///
	/// Used to signal that a writer is waiting.
	/// Prevents reads & writes.
	PendingWrite { readers: u32 },
	/// Prevent reads and writes.
	Write,
	/// Prevent flushing.
	Flush,
}

pub struct FlushLock<'a> {
	data: &'a RefCell<CacheData>,
	id: u64,
}

impl<'a> FlushLock<'a> {
	// Attempt to acquire a flush lock
	pub fn new(data: &'a RefCell<CacheData>, id: u64) -> impl Future<Output = FlushLock<'a>> + 'a {
		future::poll_fn(move |cx| {
			let mut d = data.borrow_mut();
			let tree = d.data.get_mut(&id).expect("cache entry by id not present");
			match &tree.lock {
				Lock::None => {
					tree.lock = Lock::Flush;
					Poll::Ready(Self { data, id })
				}
				_ => {
					tree.wakers.push(cx.waker().clone());
					Poll::Pending
				}
			}
		})
	}
}

impl Drop for FlushLock<'_> {
	fn drop(&mut self) {
		let mut data = self.data.borrow_mut();
		let tree = data
			.data
			.get_mut(&self.id)
			.expect("cache entry by id not present");
		tree.lock = Lock::None;
		// Take so we free the allocated memory too.
		mem::take(&mut tree.wakers)
			.into_iter()
			.for_each(|w| w.wake());
	}
}

/// Formatter for [`TreeData`].
///
/// The output is more compact than that of `derive(Debug)`, especially for large amounts of data.
pub struct FmtTreeData<'a> {
	pub data: &'a TreeData,
	pub id: u64,
}

impl fmt::Debug for FmtTreeData<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		struct FmtRecord<'a>(&'a Entry);

		impl fmt::Debug for FmtRecord<'_> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				let mut f = f.debug_map();
				let mut i = 0;
				let mut index = 0;
				while i < self.0.data.len() {
					let mut rec = Record::default();
					let l = (self.0.data.len() - i).min(mem::size_of::<Record>());
					rec.as_mut()[..l].copy_from_slice(&self.0.data[i..][..l]);
					if rec != Record::default() {
						f.entry(&index, &rec);
					}
					i += l;
					index += 1;
				}
				f.finish()
			}
		}

		struct FmtRecordMap<'a>(&'a FxHashMap<u64, Entry>);

		impl fmt::Debug for FmtRecordMap<'_> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				let mut f = f.debug_map();
				for (k, v) in self.0.iter() {
					f.entry(k, &FmtRecord(v));
				}
				f.finish()
			}
		}

		struct FmtData<'a>(&'a FmtTreeData<'a>);

		impl fmt::Debug for FmtData<'_> {
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
					f.entry(&i, &FmtRecordMap(l));
				}
				f.finish()
			}
		}

		f.debug_struct(stringify!(TreeData))
			.field("data", &FmtData(self))
			.field("lock", &self.data.lock)
			.field("wakers", &self.data.wakers)
			.finish()
	}
}
