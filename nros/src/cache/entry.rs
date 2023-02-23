use {
	super::{
		mem::{BusyState, Idx, IDX_NONE},
		Cache, IdKey, Key, Mem,
	},
	crate::{
		resource::Buf,
		util::{self, BTreeMapExt},
		waker_queue, Dev, MaxRecordSize, Resource,
	},
	alloc::collections::{BTreeMap, BTreeSet},
	core::{cell::RefMut, fmt, future::Future, ops::Deref},
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
	pub(super) key: IdKey,
	entry: RefMut<'a, Entry<B>>,
	pub(super) dirty_records: RefMut<'a, BTreeSet<Key>>,
	mem: RefMut<'a, Mem>,
	max_rec_size: MaxRecordSize,
}

impl<B: Buf> Drop for EntryRef<'_, B> {
	fn drop(&mut self) {
		if self.mem.busy.decr(self.key) {
			let diff = (1 << self.max_rec_size.to_raw()) - self.len();
			self.entry.lru_idx = self.mem.max_to_exact(self.max_rec_size, self.key, diff);
		}
	}
}

impl<'a, B: Buf> EntryRef<'a, B> {
	/// Write data to the entry.
	///
	/// # Panics
	///
	/// `offset + data.len()` may not be greater than the maximum record size.
	pub(super) fn write(&self, offset: usize, data: &[u8]) {
		trace!(
			"EntryRef::write {:?} offset {} len {}",
			self.key,
			offset,
			data.len()
		);
		assert!(
			offset + data.len() <= 1 << self.max_rec_size.to_raw(),
			"offset + data.len() is greater than max record size"
		);

		let total_len = data.len(); // Length including zeroes.
		let data = util::slice_trim_zeros_end(data);

		if entry.data.len() < offset + data.len() {
			let old_len = entry.data.len();
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
		}

		self.dirty_records.insert(key.key);
	}

	/// Write zeroes to the entry.
	///
	/// # Panics
	///
	/// `offset + data.len()` may not be greater than the maximum record size.
	pub(super) fn write_zeros(&self, offset: usize, length: usize) {
		trace!(
			"EntryRef::write_zeros {:?} offset {} len {}",
			self.key,
			offset,
			length
		);
		assert!(
			offset.saturating_add(length) <= 1 << self.max_rec_size.to_raw(),
			"offset + length is greater than max record size"
		);

		if self.len() < offset {
			// Nothing to do, as the tail is implicitly all zeroes.
			return;
		}

		if self.len() <= offset + length {
			entry.data.resize(offset, 0);
			util::trim_zeros_end(&mut entry.data);
		} else {
			entry.data.get_mut()[offset..offset + length].fill(0);
			debug_assert_ne!(entry.data.get().last(), Some(&0), "entry not trimmed");
		}

		self.dirty_records.insert(key.key);
	}

	/// Replace entry data.
	///
	/// # Panics
	///
	/// `data.len()` may not be greater than the maximum record size.
	pub(super) fn replace(&self, mut data: R::Buf) {
		trace!("EntryRef::replace {:?} len {}", self.key, data.len());
		assert!(
			data.len() <= 1 << self.max_rec_size.to_raw(),
			"data.len() is greater than max record size"
		);
		util::trim_zeros_end(&mut data);
		if data.len() > 0 || self.entry.data.len() > 0 {
			self.entry.data = data;
			self.dirty_records.insert(key.key);
		}
	}
}

impl<'a, B: Buf> Deref for EntryRef<'a, B> {
	type Target = B;

	fn deref(&self) -> &Self::Target {
		&self.entry.data
	}
}

impl<D: Dev, R: Resource> Cache<D, R> {
	async fn wait<'a, F>(&'a self, key: IdKey, not_present: F) -> Option<EntryRef<'a, R::Buf>>
	where
		F: Future<Output = ()>,
	{
		let state = self.mem().busy.incr(key);
		match state {
			BusyState::New => {
				let entry = self
					.data()
					.objects
					.get(&key.id)
					.and_then(|o| o.records.get(&key.key))
					.map(|e| (e.lru_idx, e.data.len()));
				if let Some((idx, len)) = entry {
					self.mem_idle_to_busy(idx, len).await;
					Some(self.entry_get(key).expect("no entry"))
				} else {
					not_present.await;
					None
				}
			}
			BusyState::WaitMem => {
				waker_queue::poll(|cx| {
					if let Some(ticket) = self.mem().busy.poll(key, cx.waker()) {
						Err(ticket)
					} else {
						Ok(Some(self.entry_get(key).expect("no entry")))
					}
				})
				.await
			}
			BusyState::Ready => Some(self.entry_get(key).expect("no entry")),
		}
	}

	/// Try to get an entry directly.
	///
	/// This will block if a task is busy with the entry.
	///
	/// # Note
	///
	/// This function will always move an entry to the Active state.
	/// If no entry is returned it is the caller's responsibility to ensure
	/// either an entry is inserted or a transition to the Not Present state occurs.
	pub(super) async fn wait_entry<'a>(&'a self, key: IdKey) -> Option<EntryRef<'a, R::Buf>> {
		trace!("wait_entry {:?}", key);
		self.wait(key, self.mem_empty_to_max()).await
	}

	/// Wait for an entry to not be busy.
	///
	/// Unlike [`wait_entry`] this will not cause the entry to be refetched if evicted.
	pub(super) async fn wait_entry_nofetch(&self, key: IdKey) -> Option<EntryRef<'_, R::Buf>> {
		trace!("wait_entry_nofetch {:?}", key);
		self.wait(key, async {
			self.mem().busy.decr(key);
		})
		.await
	}

	/// # Panics
	///
	/// If already present.
	///
	/// If no busy entry is present.
	pub(super) fn entry_insert<'a>(&'a self, key: IdKey, data: R::Buf) -> EntryRef<'a, R::Buf> {
		debug_assert!(self.mem().busy.has(key), "not busy");

		let (objs, mem) = RefMut::map_split(self.data(), |d| (&mut d.objects, &mut d.mem));
		let (records, dirty_records) = RefMut::map_split(objs, |o| {
			let obj = o.entry(key.id).or_default();
			(&mut obj.records, &mut obj.dirty_records)
		});
		let entry = RefMut::map(records, |rec| {
			rec.try_insert(key.key, Entry { data, lru_idx: IDX_NONE })
				.expect("entry already present")
		});

		EntryRef { key, entry, dirty_records, mem, max_rec_size: self.max_rec_size() }
	}

	/// Returns entry and whether it is dirty.
	///
	/// # Panics
	///
	/// If not present.
	#[must_use = "entry"]
	pub(super) fn entry_remove(&self, key: IdKey) -> (Entry<R::Buf>, bool) {
		let mut d = self.data();

		let mut obj = d.objects.occupied(key.id).expect("no entry");
		let entry = obj.get_mut().records.remove(&key.key).expect("no entry");
		let is_dirty = obj.get_mut().dirty_records.contains(&key.key);

		if obj.get().records.is_empty() && obj.get().dirty_records.is_empty() {
			obj.remove();
		}

		(entry, is_dirty)
	}

	/// # Panics
	///
	/// If not present.
	pub(super) fn entry_unmark_dirty(&self, key: IdKey) {
		let mut d = self.data();
		let mut obj = d.objects.occupied(key.id).expect("no entry");
		obj.get_mut().dirty_records.remove(&key.key);

		if obj.get().records.is_empty() && obj.get().dirty_records.is_empty() {
			obj.remove();
		}
	}

	/// Get an already present entry.
	///
	/// If `is_referenced` is `true`, the reference count to the entry is decreased if present.
	fn entry_get<'a>(&'a self, key: IdKey) -> Option<EntryRef<'a, R::Buf>> {
		let (objs, mut mem) = RefMut::map_split(self.data(), |d| (&mut d.objects, &mut d.mem));

		let obj = RefMut::filter_map(objs, |o| o.get_mut(&key.id)).ok()?;
		let (records, dirty_records) =
			RefMut::map_split(obj, |o| (&mut o.records, &mut o.dirty_records));

		let mut entry = RefMut::filter_map(records, |r| r.get_mut(&key.key)).ok()?;

		mem.tracker.touch(entry.lru_idx);

		Some(EntryRef { key, entry, dirty_records, mem, max_rec_size: self.max_rec_size() })
	}
}
