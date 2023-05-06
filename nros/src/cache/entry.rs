use {
	super::{
		mem::{Idx, IDX_NONE},
		Cache, IdKey,
	},
	crate::{resource::Buf, util, waker_queue, Dev, Resource},
	alloc::collections::BTreeSet,
	core::{
		cell::{RefCell, RefMut},
		fmt,
		mem::ManuallyDrop,
	},
};

pub(super) struct Entry<B: Buf> {
	/// Unpacked record data.
	pub data: B,
	/// Index in LRU.
	///
	/// Invalid if any busy entries for this entry are present.
	pub lru_idx: Idx,
}

impl<B: Buf> fmt::Debug for Entry<B> {
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		format_args!("{:?} @ {:?}", self.lru_idx, self.data.get()).fmt(f)
	}
}

/// Reference to an entry.
pub struct EntryRef<'a, B: Buf> {
	entry: ManuallyDrop<RefMut<'a, Entry<B>>>,
	pub(super) key: IdKey,
	pub(super) dirty: ManuallyDrop<RefMut<'a, BTreeSet<IdKey>>>,
	cache: &'a RefCell<super::CacheData<B>>,
	in_lru: bool,
}

impl<B: Buf> Drop for EntryRef<'_, B> {
	fn drop(&mut self) {
		let lru_idx = self.entry.lru_idx;
		// SAFETY: no other code will be able to touch entry or dirty
		unsafe {
			ManuallyDrop::drop(&mut self.entry);
			ManuallyDrop::drop(&mut self.dirty);
		}
		let mut d = self.cache.borrow_mut();
		if d.busy.decr(self.key) {
			if !self.in_lru {
				let idx = d.mem.soft_add(self.key);
				let entry = d.records.get_mut(&self.key).expect("no entry");
				debug_assert_eq!(entry.lru_idx, IDX_NONE, "entry already in LRU");
				entry.lru_idx = idx;
			}
		} else if self.in_lru {
			d.mem.soft_del(lru_idx);
			let entry = d.records.get_mut(&self.key).expect("no entry");
			debug_assert_ne!(entry.lru_idx, IDX_NONE, "entry still in LRU");
			entry.lru_idx = IDX_NONE;
		}
	}
}

impl<'a, B: Buf> EntryRef<'a, B> {
	/// Write data to the entry.
	pub(super) fn write(&mut self, offset: usize, data: &[u8]) {
		trace!(
			"EntryRef::write {:?} offset {} len {}",
			self.key,
			offset,
			data.len()
		);

		let total_len = data.len(); // Length including zeroes.
		let data = util::slice_trim_zeros_end(data);

		let entry = &mut self.entry;
		if !data.is_empty() && entry.data.len() < offset + data.len() {
			entry.data.resize(offset, 0);
			entry.data.extend_from_slice(data);
		} else if offset + total_len < entry.data.len() {
			// Write in-place.
			let d = &mut entry.data.get_mut()[offset..offset + total_len];
			d[..data.len()].copy_from_slice(data);
			d[data.len()..].fill(0);
		} else {
			// If the end would be filled with zeroes, we can shrink.
			entry.data.resize(offset, 0);
			entry.data.extend_from_slice(data);
			util::trim_zeros_end(&mut entry.data);
		}
		debug_assert_ne!(entry.data.get().last(), Some(&0), "entry not trimmed");

		self.dirty.insert(self.key);
	}

	/// Write zeroes to the entry.
	pub(super) fn write_zeros(&mut self, offset: usize, length: usize) {
		trace!(
			"EntryRef::write_zeros {:?} offset {} len {}",
			self.key,
			offset,
			length
		);

		let entry = &mut self.entry;
		if entry.data.len() < offset {
			// Nothing to do, as the tail is implicitly all zeroes.
			return;
		}

		if entry.data.len() <= offset + length {
			entry.data.resize(offset, 0);
			util::trim_zeros_end(&mut entry.data);
		} else {
			entry.data.get_mut()[offset..offset + length].fill(0);
			debug_assert_ne!(entry.data.get().last(), Some(&0), "entry not trimmed");
		}

		self.dirty.insert(self.key);
	}

	/// Replace entry data.
	pub(super) fn replace(&mut self, mut data: B) {
		trace!("EntryRef::replace {:?} len {}", self.key, data.len());
		util::trim_zeros_end(&mut data);
		if data.len() > 0 || self.entry.data.len() > 0 {
			self.entry.data = data;
			self.dirty.insert(self.key);
		}
	}

	/// Read data.
	pub(super) fn read(&self, offset: usize, buf: &mut [u8]) {
		trace!(
			"EntryRef::read {:?} offset {} len {}",
			self.key,
			offset,
			buf.len()
		);
		util::read(offset, buf, self.entry.data.get());
	}

	pub(super) fn as_slice(&self) -> &[u8] {
		self.entry.data.get()
	}

	pub(super) fn len(&self) -> usize {
		self.entry.data.len()
	}
}

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Try to get an entry directly.
	///
	/// This will block if a task is busy with the entry.
	///
	/// # Note
	///
	/// This function will always increase the reference count to an entry.
	/// If no entry is returned it is the caller's responsibility to ensure
	/// either an entry is inserted or a transition to the Not Present state occurs.
	pub(super) async fn wait_entry(&self, key: IdKey) -> Option<EntryRef<'_, R::Buf>> {
		trace!("wait_entry {:?}", key);
		if self.data().busy.incr(key) {
			self.entry_get(key)
		} else {
			waker_queue::poll(|cx| {
				if let Some(rec) = self.entry_get(key) {
					return Ok(Some(rec));
				}
				Err(self.data().busy.wait(key, cx.waker().clone()))
			})
			.await
		}
	}

	/// # Panics
	///
	/// If already present.
	pub(super) fn entry_insert(&self, key: IdKey, data: R::Buf) -> EntryRef<'_, R::Buf> {
		let (recs, dirty) = RefMut::map_split(self.data(), |d| (&mut d.records, &mut d.dirty));
		let rec = RefMut::map(recs, |r| {
			r.try_insert(key, Entry { data, lru_idx: IDX_NONE })
				.expect("entry already present")
		});
		EntryRef {
			key,
			in_lru: rec.lru_idx != IDX_NONE,
			entry: ManuallyDrop::new(rec),
			dirty: ManuallyDrop::new(dirty),
			cache: &self.data,
		}
	}

	/// Returns entry and whether it is dirty.
	///
	/// # Panics
	///
	/// If not present.
	#[must_use = "entry"]
	pub(super) fn entry_remove(&self, key: IdKey) -> (R::Buf, bool) {
		let mut d = self.data();
		let entry = d.records.remove(&key).expect("no entry");
		let is_dirty = d.dirty.contains(&key);
		if entry.lru_idx != IDX_NONE {
			d.mem.soft_del(entry.lru_idx);
		}
		(entry.data, is_dirty)
	}

	/// Get an already present entry.
	///
	/// If `is_referenced` is `true`, the reference count to the entry is decreased if present.
	fn entry_get(&self, key: IdKey) -> Option<EntryRef<'_, R::Buf>> {
		let (recs, dirty) = RefMut::map_split(self.data(), |d| (&mut d.records, &mut d.dirty));
		let rec = RefMut::filter_map(recs, |r| r.get_mut(&key)).ok()?;
		Some(EntryRef {
			key,
			in_lru: rec.lru_idx != IDX_NONE,
			entry: ManuallyDrop::new(rec),
			dirty: ManuallyDrop::new(dirty),
			cache: &self.data,
		})
	}
}
