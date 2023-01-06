use {
	super::{lru, Cache, Key, Lrus, CACHE_ENTRY_FIXED_COST},
	crate::{resource::Buf, util::trim_zeros_end, Dev, Error, Resource},
	core::{cell::RefMut, fmt, ops::Deref},
};

/// A single cache entry.
pub struct Entry<R: Resource> {
	/// The data itself.
	pub data: R::Buf,
	/// Global LRU index.
	pub global_index: lru::Idx,
	/// Dirty LRU index, if the data is actually dirty.
	pub write_index: Option<lru::Idx>,
}

impl<R: Resource> fmt::Debug for Entry<R> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Entry))
			.field("data", &format_args!("{:?}", &self.data.get()))
			.field("global_index", &self.global_index)
			.field("write_index", &self.write_index)
			.finish()
	}
}

/// Reference to an entry.
pub struct EntryRef<'a, D: Dev, R: Resource> {
	cache: &'a Cache<D, R>,
	key: Key,
	lrus: RefMut<'a, Lrus>,
	entry: RefMut<'a, Entry<R>>,
}

impl<'a, D: Dev, R: Resource> EntryRef<'a, D, R> {
	/// Construct a new [`EntryRef`] for the given entry.
	pub(super) fn new(
		cache: &'a Cache<D, R>,
		key: Key,
		entry: RefMut<'a, Entry<R>>,
		lrus: RefMut<'a, Lrus>,
	) -> Self {
		Self { cache, key, entry, lrus }
	}

	/// Modify the entry's data.
	///
	/// This may trigger a flush when the closure returns.
	///
	/// This consumes the entry to ensure no reference is held across an await point.
	pub async fn modify(self, f: impl FnOnce(&mut R::Buf)) -> Result<(), Error<D>> {
		let Self { cache, key, mut lrus, mut entry } = self;
		let original_len = entry.data.len();

		// Apply modifications.
		f(&mut entry.data);
		// Trim zeros, which we always want to do.
		trim_zeros_end(&mut entry.data);

		// Check if we still need to mark the entry as dirty.
		// Otherwise promote.
		lrus.global.cache_size += entry.data.len();
		lrus.global.cache_size -= original_len;
		if let Some(idx) = entry.write_index {
			lrus.dirty.lru.promote(idx);
			lrus.dirty.cache_size += entry.data.len();
			lrus.dirty.cache_size -= original_len;
			drop((lrus, entry));
		} else {
			let idx = lrus.dirty.lru.insert(key);
			lrus.dirty.cache_size += entry.data.len() + CACHE_ENTRY_FIXED_COST;
			entry.write_index = Some(idx);
			drop((lrus, entry));
			// Update dirty counters
			let mut data = self.cache.data.borrow_mut();
			let levels = &mut data.data.get_mut(&key.id()).unwrap().data;
			let mut offt = key.offset();
			for level in levels[key.depth().into()..].iter_mut() {
				*level.dirty_counters.entry(offt).or_insert(0) += 1;
				offt >>= cache.entries_per_parent_p2();
			}
		}

		// Flush
		cache.flush().await
	}
}

impl<'a, D: Dev, R: Resource> Deref for EntryRef<'a, D, R> {
	type Target = Entry<R>;

	fn deref(&self) -> &Self::Target {
		&self.entry
	}
}

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Try to get an entry directly.
	pub(super) fn get_entry(&self, key: Key) -> Option<EntryRef<'_, D, R>> {
		let data = self.data.borrow_mut();

		let (trees, lrus) = RefMut::map_split(data, |d| (&mut d.data, &mut d.lrus));
		let tree = RefMut::filter_map(trees, |t| t.get_mut(&key.id())).ok()?;
		let levels = RefMut::map(tree, |t| &mut t.data[usize::from(key.depth())]);

		RefMut::filter_map(levels, |l| l.entries.get_mut(&key.offset()))
			.map(|entry| EntryRef::new(self, key, entry, lrus))
			.ok()
	}
}
