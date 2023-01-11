use {
	super::{lru, Cache, Key, Lrus},
	crate::{resource::Buf, util::trim_zeros_end, Background, Dev, Error, Resource},
	core::{cell::RefMut, fmt, ops::Deref},
	std::collections::hash_map,
};

/// A single cache entry.
pub struct Entry<R: Resource> {
	/// The data itself.
	pub data: R::Buf,
	/// Global LRU index.
	pub global_index: lru::Idx,
}

impl<R: Resource> fmt::Debug for Entry<R> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Entry))
			.field("data", &format_args!("{:?}", &self.data.get()))
			.field("global_index", &self.global_index)
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
	pub async fn modify(
		self,
		bg: &Background<'a, D>,
		f: impl FnOnce(&mut R::Buf),
	) -> Result<(), Error<D>> {
		let Self { cache, key, mut lrus, mut entry } = self;
		let original_len = entry.data.len();

		// Apply modifications.
		f(&mut entry.data);
		// Trim zeros, which we always want to do.
		trim_zeros_end(&mut entry.data);

		// Adjust cache use
		lrus.global.cache_size += entry.data.len();
		lrus.global.cache_size -= original_len;

		// Bump entry to front of LRU.
		lrus.global.lru.promote(entry.global_index);

		// Update dirty counters if not already dirty.
		drop((lrus, entry));
		let mut data = self.cache.data.borrow_mut();
		let [level, levels @ ..] = &mut data.data.get_mut(&key.id()).unwrap().data[usize::from(key.depth())..]
			else { panic!("depth out of range") };
		let counter = level.dirty_counters.entry(key.offset()).or_insert(0);
		if *counter & isize::MIN == 0 {
			*counter |= isize::MIN;
			*counter += 1;

			let mut offt = key.offset() >> cache.entries_per_parent_p2();
			for lvl in levels {
				*lvl.dirty_counters.entry(offt).or_insert(0) += 1;
				offt >>= cache.entries_per_parent_p2();
			}
		}
		drop(data);

		// Flush
		cache.flush(bg).await
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
