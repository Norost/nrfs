use {
	super::{tree::data::Dirty, Busy, Cache, Key, MemoryTracker, Present, Slot},
	crate::{resource::Buf, util, waker_queue, Dev, Resource},
	alloc::{collections::BTreeMap, rc::Rc},
	core::{
		cell::{RefCell, RefMut},
		ops::Deref,
	},
};

/// Reference to an entry.
pub struct EntryRef<'a, D: Dev, R: Resource> {
	cache: &'a Cache<D, R>,
	pub(super) key: Key,
	memory_tracker: RefMut<'a, MemoryTracker>,
	entry: RefMut<'a, Present<R::Buf>>,
	pub(super) dirty_markers: RefMut<'a, BTreeMap<u64, Dirty>>,
}

impl<'a, D: Dev, R: Resource> EntryRef<'a, D, R> {
	/// Construct a new [`EntryRef`] for the given entry.
	///
	/// This puts the entry at the back of the LRU queue.
	pub(super) fn new(
		cache: &'a Cache<D, R>,
		key: Key,
		entry: RefMut<'a, Present<R::Buf>>,
		dirty_markers: RefMut<'a, BTreeMap<u64, Dirty>>,
		mut memory_tracker: RefMut<'a, MemoryTracker>,
	) -> Self {
		memory_tracker.touch(&entry.refcount);
		Self { cache, key, entry, dirty_markers, memory_tracker }
	}

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

		let Self { cache, key, mut entry, mut memory_tracker, dirty_markers } = self;
		drop(dirty_markers);

		let rec_size = 1 << self.cache.max_record_size().to_raw();
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
			memory_tracker.force_grow(&entry.refcount, entry.data.len(), offset + data.len());

			// Write data
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
			memory_tracker.shrink(&entry.refcount, old_len, entry.data.len());
		}

		// Mark dirty
		drop((entry, memory_tracker));
		let (mut obj, _) = cache.get_object(key.id()).expect("no object");
		obj.data
			.mark_dirty(key.depth(), key.offset(), cache.max_record_size());

		drop(obj);
		#[cfg(test)]
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

		let Self { cache, key, mut entry, dirty_markers, mut memory_tracker } = self;
		drop(dirty_markers);

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

			memory_tracker.shrink(&entry.refcount, entry_len, entry.data.len());
		} else {
			// Fill
			entry.data.get_mut()[offset..offset + length].fill(0);
		}
		debug_assert_ne!(entry.data.get().last(), Some(&0), "entry not trimmed");

		// Mark dirty
		drop((entry, memory_tracker));
		let (mut obj, _) = cache.get_object(key.id()).expect("no object");
		obj.data
			.mark_dirty(key.depth(), key.offset(), cache.max_record_size());

		drop(obj);
		#[cfg(test)]
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

		let Self { cache, key, mut entry, dirty_markers, mut memory_tracker } = self;
		drop(dirty_markers);

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
			memory_tracker.force_grow(&entry.refcount, entry.data.len(), data.len());
			entry.data = data;
		} else {
			memory_tracker.shrink(&entry.refcount, entry.data.len(), data.len());
			entry.data = data;
		}

		// Mark dirty
		drop((memory_tracker, entry));
		let (mut obj, _) = cache.get_object(key.id()).expect("no object");
		obj.data
			.mark_dirty(key.depth(), key.offset(), cache.max_record_size());

		drop(obj);
		#[cfg(test)]
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
	pub(super) async fn wait_entry(&self, mut key: Key) -> Option<EntryRef<'_, D, R>> {
		trace!("wait_entry {:?}", key);
		let mut busy = None::<Rc<RefCell<Busy>>>;
		waker_queue::poll(|cx| {
			if let Some(busy) = busy.as_mut() {
				key = busy.borrow_mut().key;
			}

			let Some(mut comp) = self.get_entryref_components(key) else {
				debug_assert!(busy.is_none(), "entry {:?} removed while waiting", key);
				return Ok(None)
			};

			let mut ticket = None;
			let entry = RefMut::filter_map(comp.slot, |slot| match slot {
				Slot::Present(entry) => {
					if busy.is_some() {
						comp.memory_tracker.decr_entry_refcount(
							&mut entry.refcount,
							entry.data.len(),
							1,
						);
					}
					Some(entry)
				}
				Slot::Busy(entry) => {
					let mut e = entry.borrow_mut();
					ticket = Some(e.wakers.push(cx.waker().clone(), ()));
					if busy.is_none() {
						e.refcount += 1;
					}
					busy = Some(entry.clone());
					None
				}
			});
			if let Some(ticket) = ticket {
				Err(ticket)
			} else if let Ok(entry) = entry {
				Ok(Some(EntryRef::new(
					self,
					key,
					entry,
					comp.dirty_markers,
					comp.memory_tracker,
				)))
			} else {
				Ok(None)
			}
		})
		.await
	}

	/// Get [`RefMut`]s to components necessary to construct a [`EntryRef`].
	pub(super) fn get_entryref_components(&self, key: Key) -> Option<EntryRefComponents<'_, R>> {
		let data = self.data.borrow_mut();

		let (objects, memory_tracker) =
			RefMut::map_split(data, |d| (&mut d.objects, &mut d.memory_tracker));

		let level = RefMut::filter_map(objects, |objects| {
			let slot = objects.get_mut(&key.id())?;
			let Slot::Present(obj) = slot else { return None };
			Some(obj.level_mut(key.depth()))
		})
		.ok()?;

		let (slots, dirty_markers) =
			RefMut::map_split(level, |level| (&mut level.slots, &mut level.dirty_markers));

		let slot = RefMut::filter_map(slots, |slots| slots.get_mut(&key.offset())).ok()?;

		Some(EntryRefComponents { memory_tracker, slot, dirty_markers })
	}
}

pub(super) struct EntryRefComponents<'a, R: Resource> {
	pub memory_tracker: RefMut<'a, MemoryTracker>,
	pub slot: RefMut<'a, Slot<R::Buf>>,
	pub dirty_markers: RefMut<'a, BTreeMap<u64, Dirty>>,
}
