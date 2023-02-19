use {
	super::{memory_tracker::Idx, Cache, IdKey, Key, MemoryTracker},
	crate::{
		resource::Buf,
		util::{self, BTreeMapExt},
		waker_queue, Dev, Resource,
	},
	alloc::collections::BTreeSet,
	core::{cell::RefMut, fmt, ops::Deref},
};

pub(super) struct Entry<B: Buf> {
	/// Unpacked record data.
	pub data: B,
	/// LRU index or amount of references.
	pub lru_ref: LruRef,
}

impl<B: Buf> fmt::Debug for Entry<B> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Entry))
			.field("data", &format_args!("{:?}", self.data.get()))
			.field("lru_ref", &self.lru_ref)
			.finish()
	}
}

/// Reference counter to prevent slots from transitioning to
/// empty or flushing state before all tasks have finished with it.
#[derive(Debug)]
pub(super) enum LruRef {
	/// There are tasks remaining.
	Ref {
		/// The amount of tasks referencing this data.
		refcount: usize,
	},
	/// There are no remaining entries.
	NoRef {
		/// The position in the LRU.
		lru_index: Idx,
	},
}

/// Reference to an entry.
pub struct EntryRef<'a, D: Dev, R: Resource> {
	cache: &'a Cache<D, R>,
	pub(super) key: IdKey,
	entry: RefMut<'a, Entry<R::Buf>>,
	pub(super) dirty_records: RefMut<'a, BTreeSet<Key>>,
	memory_tracker: RefMut<'a, MemoryTracker>,
}

impl<'a, D: Dev, R: Resource> EntryRef<'a, D, R> {
	/// Write data to the entry.
	///
	/// This consumes the entry to ensure no reference is held across an await point.
	///
	/// # Note
	///
	/// `offset + data.len()` may not be greater than the maximum record size.
	pub(super) async fn write(self, offset: usize, data: &[u8]) {
		trace!(
			"EntryRef::write {:?} offset {} len {}",
			self.key,
			offset,
			data.len()
		);

		let Self { cache, key, mut entry, mut dirty_records, mut memory_tracker } = self;

		let rec_size = 1 << cache.max_record_size().to_raw();
		debug_assert!(
			offset + data.len() <= rec_size,
			"offset + data.len() is greater than max record size"
		);

		let total_len = data.len(); // Length including zeroes.
		let data = util::slice_trim_zeros_end(data);

		if entry.data.len() < offset + data.len() {
			// We'll have to grow the buffer, ensure memory is available for it.
			// FIXME we *must* somehow reserve memory without await.
			// For now, cheat and go past the limit.
			memory_tracker.force_grow(&entry.lru_ref, entry.data.len(), offset + data.len());

			entry.data.resize(offset, 0);
			entry.data.extend_from_slice(data);
		} else if offset + total_len < entry.data.len() {
			// Write in-place.
			let d = &mut entry.data.get_mut()[offset..offset + total_len];
			d[..data.len()].copy_from_slice(data);
			d[data.len()..].fill(0);
		} else {
			// If the end would be filled with zeroes, we can shrink.
			let old_len = entry.data.len();
			entry.data.resize(offset, 0);
			entry.data.extend_from_slice(data);
			util::trim_zeros_end(&mut entry.data);

			memory_tracker.shrink(&entry.lru_ref, old_len, entry.data.len());
		}

		// Mark dirty
		dirty_records.insert(key.key);

		drop((entry, dirty_records, memory_tracker));
		cache.verify_cache_usage();

		// Do try to stay as close as the limit as possible.
		cache.memory_compensate_forced_grow().await
	}

	/// Write zeroes to the entry.
	///
	/// This consumes the entry for consistency with [`write`] and [`replace`].
	///
	/// # Note
	///
	/// `offset + data.len()` may not be greater than the maximum record size.
	pub(super) fn write_zeros(self, offset: usize, length: usize) {
		trace!(
			"EntryRef::write_zeros {:?} offset {} len {}",
			self.key,
			offset,
			length
		);

		let Self { cache, key, mut entry, dirty_records, mut memory_tracker } = self;
		drop(dirty_records);

		let rec_size = 1 << cache.max_record_size().to_raw();
		assert!(
			offset.saturating_add(length) <= rec_size,
			"offset + length is greater than max record size"
		);

		let entry_len = entry.data.len();

		if entry_len < offset {
			// Nothing to do, as the tail is implicitly all zeroes.
			return;
		}

		if entry_len <= offset + length {
			// Trim
			entry.data.resize(offset, 0);
			util::trim_zeros_end(&mut entry.data);

			memory_tracker.shrink(&entry.lru_ref, entry_len, entry.data.len());
		} else {
			// Fill
			entry.data.get_mut()[offset..offset + length].fill(0);
		}
		debug_assert_ne!(entry.data.get().last(), Some(&0), "entry not trimmed");

		// Mark dirty
		drop((entry, memory_tracker));
		cache
			.data
			.borrow_mut()
			.objects
			.get_mut(&key.id)
			.expect("no object")
			.dirty_records
			.insert(key.key);

		cache.verify_cache_usage();
	}

	/// Replace entry data.
	///
	/// This consumes the entry to ensure no reference is held across an await point.
	///
	/// # Note
	///
	/// `data.len()` may not be greater than the maximum record size.
	pub(super) async fn replace(self, mut data: R::Buf) {
		trace!("EntryRef::replace {:?} len {}", self.key, data.len());

		let Self { cache, key, mut entry, mut dirty_records, mut memory_tracker } = self;

		let rec_size = 1 << cache.max_record_size().to_raw();
		assert!(
			data.len() <= rec_size,
			"data.len() is greater than max record size"
		);

		// Trim
		util::trim_zeros_end(&mut data);

		if entry.data.len() < data.len() {
			// We'll have to grow the buffer, ensure memory is available for it.
			// FIXME we *must* somehow reserve memory without await.
			// For now, cheat and go past the limit.
			memory_tracker.force_grow(&entry.lru_ref, entry.data.len(), data.len());
		} else {
			memory_tracker.shrink(&entry.lru_ref, entry.data.len(), data.len());
		}
		entry.data = data;

		// Mark dirty
		dirty_records.insert(key.key);
		drop((memory_tracker, entry, dirty_records));

		cache.verify_cache_usage();

		// Do try to stay as close as the limit as possible.
		cache.memory_compensate_forced_grow().await
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
	///
	/// This will block if a task is busy with the entry.
	pub(super) async fn wait_entry(&self, key: IdKey) -> Option<EntryRef<'_, D, R>> {
		trace!("wait_entry {:?}", key);
		let mut referenced = false;
		waker_queue::poll(|cx| {
			if let Some(mut entry) = self.entry_get(key) {
				if referenced {
					let len = entry.len();
					entry
						.memory_tracker
						.decr_entry_refcount(key, &mut entry.entry.lru_ref, len, 1);
				}
				return Ok(Some(entry));
			}

			if let Some(busy) = self.data.borrow_mut().busy.get_mut(&key) {
				busy.refcount += usize::from(!referenced);
				referenced = true;
				return Err(busy.wakers.push(cx.waker().clone(), ()));
			}

			assert!(!referenced, "entry removed while waiting");
			Ok(None)
		})
		.await
	}

	/// # Panics
	///
	/// If already present.
	pub(super) fn entry_insert(
		&self,
		key: IdKey,
		data: R::Buf,
		refcount: usize,
	) -> EntryRef<'_, D, R> {
		let lru_ref = self.memory_soft_add_entry(key, refcount, data.len());

		let (objs, memory_tracker) = RefMut::map_split(self.data.borrow_mut(), |d| {
			(&mut d.objects, &mut d.memory_tracker)
		});
		let (records, dirty_records) = RefMut::map_split(objs, |o| {
			let obj = o.entry(key.id).or_default();
			(&mut obj.records, &mut obj.dirty_records)
		});

		let entry = RefMut::map(records, |rec| {
			rec.try_insert(key.key, Entry { data, lru_ref })
				.expect("entry already present")
		});

		EntryRef { cache: self, key, entry, dirty_records, memory_tracker }
	}

	/// Returns entry and whether it is dirty.
	///
	/// # Panics
	///
	/// If not present.
	#[must_use = "entry"]
	pub(super) fn entry_remove(&self, key: IdKey) -> (R::Buf, usize, bool) {
		let mut data = self.data.borrow_mut();

		let mut obj = data.objects.occupied(key.id).expect("no entry");
		let entry = obj.get_mut().records.remove(&key.key).expect("no entry");
		let is_dirty = obj.get_mut().dirty_records.contains(&key.key);

		if obj.get().records.is_empty() && obj.get().dirty_records.is_empty() {
			obj.remove();
		}

		drop(data);
		let refcount = self.memory_soft_remove_entry(entry.lru_ref, entry.data.len());

		(entry.data, refcount, is_dirty)
	}

	/// # Panics
	///
	/// If not present.
	pub(super) fn entry_unmark_dirty(&self, key: IdKey) {
		let mut d = self.data.borrow_mut();
		let mut obj = d.objects.occupied(key.id).expect("no entry");
		obj.get_mut().dirty_records.remove(&key.key);

		if obj.get().records.is_empty() && obj.get().dirty_records.is_empty() {
			obj.remove();
		}
	}

	/// Get an already present entry.
	pub(super) fn entry_get(&self, key: IdKey) -> Option<EntryRef<'_, D, R>> {
		let (objs, mut memory_tracker) = RefMut::map_split(self.data.borrow_mut(), |d| {
			(&mut d.objects, &mut d.memory_tracker)
		});

		let obj = RefMut::filter_map(objs, |o| o.get_mut(&key.id)).ok()?;
		let (records, dirty_records) =
			RefMut::map_split(obj, |o| (&mut o.records, &mut o.dirty_records));

		let entry = RefMut::filter_map(records, |r| r.get_mut(&key.key)).ok()?;

		memory_tracker.touch(&entry.lru_ref);
		Some(EntryRef { cache: self, key, entry, dirty_records, memory_tracker })
	}
}
