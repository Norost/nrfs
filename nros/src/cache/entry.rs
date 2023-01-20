use {
	super::{lru, slot, Cache, Key, Lru, Slot, CACHE_ENTRY_FIXED_COST},
	core::task::Poll,
	crate::{resource::Buf, util::trim_zeros_end, Background, Dev, Error, Resource},
	core::{future, cell::RefMut, fmt, ops::Deref},
	std::collections::hash_map,
	core::num::NonZeroUsize,
};

/// Reference to an entry.
pub struct EntryRef<'a, D: Dev, R: Resource> {
	cache: &'a Cache<D, R>,
	pub(super) key: Key,
	lru: RefMut<'a, Lru>,
	entry: RefMut<'a, slot::Present<R::Buf>>,
}

impl<'a, D: Dev, R: Resource> EntryRef<'a, D, R> {
	/// Construct a new [`EntryRef`] for the given entry.
	pub(super) fn new(
		cache: &'a Cache<D, R>,
		key: Key,
		entry: RefMut<'a, slot::Present<R::Buf>>,
		lru: RefMut<'a, Lru>,
	) -> Self {
		Self { cache, key, entry, lru }
	}

	/// Modify the entry's data.
	///
	/// This may trigger a flush when the closure returns.
	///
	/// This consumes the entry to ensure no reference is held across an await point.
	pub fn modify(self, bg: &Background<'a, D>, f: impl FnOnce(&mut R::Buf)) {
		let Self { cache, key, mut lru, mut entry } = self;
		let original_len = entry.data.len();

		// Apply modifications.
		f(&mut entry.data);
		// Trim zeros, which we always want to do.
		trim_zeros_end(&mut entry.data);

		if let slot::RefCount::NoRef { lru_index } = entry.refcount {
			// Adjust cache use
			lru.cache_size += entry.data.len();
			lru.cache_size -= original_len;

			// Bump entry to front of LRU.
			lru.lru.promote(lru_index);

			// Update dirty counters if not already dirty.
			drop((lru, entry));
			let mut data = self.cache.data.borrow_mut();
			let Some(Slot::Present(obj)) = data.objects.get_mut(&key.id())
				else { panic!("no object") };
			obj.data
				.mark_dirty(key.depth(), key.offset(), cache.max_record_size());
			drop(data);

			// Flush
			cache.evict_excess(bg);
		}
	}
}

impl<'a, D: Dev, R: Resource> Deref for EntryRef<'a, D, R> {
	type Target = R::Buf;

	fn deref(&self) -> &Self::Target {
		&self.entry.data
	}
}

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Try to get an entry directly.
	pub(super) fn get_entry(&self, key: Key) -> Option<EntryRef<'_, D, R>> {
		let data = self.data.borrow_mut();

		let (trees, lru) = RefMut::map_split(data, |d| (&mut d.objects, &mut d.lru));

		let entry = RefMut::filter_map(trees, |t| {
			let slot = t.get_mut(&key.id())?;
			let Slot::Present(tree) = slot else { return None };
			let level = &mut tree.data.data[usize::from(key.depth())];
			let slot = level.slots.get_mut(&key.offset())?;
			let Slot::Present(entry) = &mut *slot else { return None };
			Some(entry)
		})
		.ok()?;

		Some(EntryRef::new(self, key, entry, lru))
	}

	/// Try to get an entry directly.
	///
	/// This will block if a task is busy with the entry.
	pub(super) async fn wait_entry(&self, key: Key) -> Option<EntryRef<'_, D, R>> {
		let mut is_referenced = false;
		future::poll_fn(|cx| {
			let data = self.data.borrow_mut();
			let (trees, mut lru) = RefMut::map_split(data, |d| (&mut d.objects, &mut d.lru));
			let entry = RefMut::filter_map(trees, |t| {
				let slot = t.get_mut(&key.id())?;
				let Slot::Present(tree) = slot else { return None };
				let level = &mut tree.data.data[usize::from(key.depth())];
				let entry = level.slots.get_mut(&key.offset())?;
				match entry {
					Slot::Present(present) => {
						if is_referenced {
							let len = CACHE_ENTRY_FIXED_COST + present.data.len();
							lru.decrease_refcount(&mut present.refcount, key, len);
						}
						Some(present)
					}
					Slot::Busy(busy) => {
						let mut busy = busy.borrow_mut();
						busy.wakers.push(cx.waker().clone());
						if !is_referenced {
							busy.refcount = NonZeroUsize::new(busy.refcount.map_or(0, |x| x.get()) + 1);
							is_referenced = true;
						}
						None
					}
				}
			});
			match entry {
				Ok(entry) => Poll::Ready(Some(EntryRef::new(self, key, entry, lru))),
				Err(_) if is_referenced => Poll::Pending,
				Err(_) => Poll::Ready(None),
			}
		}).await
	}
}
