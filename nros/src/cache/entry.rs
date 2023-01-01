use {
	super::{lru, Cache, Key, Lrus, CACHE_ENTRY_FIXED_COST},
	crate::{util::trim_zeros_end, Dev, Error},
	core::{cell::RefMut, fmt, ops::Deref},
	rustc_hash::FxHashMap,
};

/// A single cache entry.
pub struct Entry {
	/// The data itself.
	pub data: Vec<u8>,
	/// Global LRU index.
	pub global_index: lru::Idx,
	/// Dirty LRU index, if the data is actually dirty.
	pub write_index: Option<lru::Idx>,
}

impl fmt::Debug for Entry {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Entry))
			.field("data", &format_args!("{:?}", &self.data))
			.field("global_index", &self.global_index)
			.field("write_index", &self.write_index)
			.finish()
	}
}

/// Reference to an entry.
pub struct EntryRef<'a, D: Dev> {
	cache: &'a Cache<D>,
	key: Key,
	lrus: RefMut<'a, Lrus>,
	entry: RefMut<'a, Entry>,
}

impl<'a, D: Dev> EntryRef<'a, D> {
	/// Construct a new [`EntryRef`] for the given entry.
	pub(super) fn new(
		cache: &'a Cache<D>,
		key: Key,
		entry: RefMut<'a, Entry>,
		lrus: RefMut<'a, Lrus>,
	) -> Self {
		Self { cache, key, entry, lrus }
	}

	/// Modify the entry's data.
	///
	/// This may trigger a flush when the closure returns.
	///
	/// This consumes the entry to ensure no reference is held across an await point.
	pub async fn modify(self, f: impl FnOnce(&mut Vec<u8>)) -> Result<(), Error<D>> {
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
			for (i, level) in levels[key.depth().into()..].iter_mut().enumerate() {
				let offt = key.offset() >> usize::from(cache.max_record_size().to_raw() - 5) * i;
				*level.dirty_counters.entry(offt).or_insert(0) += 1;
			}
		}

		// Flush
		cache.flush().await
	}
}

impl<'a, D: Dev> Deref for EntryRef<'a, D> {
	type Target = Entry;

	fn deref(&self) -> &Self::Target {
		&self.entry
	}
}

impl<D: Dev> Cache<D> {
	/// Try to get an entry directly.
	pub(super) fn get_entry(&self, key: Key) -> Option<EntryRef<'_, D>> {
		let data = self.data.borrow_mut();

		let (trees, lrus) = RefMut::map_split(data, |d| (&mut d.data, &mut d.lrus));
		let tree = RefMut::filter_map(trees, |t| t.get_mut(&key.id())).ok()?;
		let levels = RefMut::map(tree, |t| &mut t.data[usize::from(key.depth())]);

		RefMut::filter_map(levels, |l| l.entries.get_mut(&key.offset()))
			.map(|entry| EntryRef::new(self, key, entry, lrus))
			.ok()
	}
}
