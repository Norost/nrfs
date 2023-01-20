use {
	super::{Key, RefCount, ID_PSEUDO, OBJECT_LIST_ID},
	core::num::NonZeroUsize,
};

pub type Gen = u8;
//pub type Gen = ();
pub type Idx = arena::Handle<Gen>;

const IDX_NONE: Idx = Idx::from_raw(usize::MAX, u8::MAX);
//const IDX_NONE: Idx = Idx::from_raw(usize::MAX, ());

/// Estimated fixed cost for every cached entry.
///
/// This is in addition to the amount of data stored by the entry.
const CACHE_ENTRY_FIXED_COST: usize = 32;

const CACHE_OBJECT_FIXED_COST: usize = 128;

/// Cache LRU queue, with tracking per byte used.
#[derive(Debug)]
pub(super) struct Lru {
	/// Linked list for LRU entries
	lru: LruList<Key>,
	/// The maximum amount of total bytes to keep cached.
	cache_max: usize,
	/// The amount of cached bytes.
	cache_size: usize,
}

impl Lru {
	pub fn new(max_size: usize) -> Self {
		Self { lru: Default::default(), cache_max: max_size, cache_size: 0 }
	}

	fn add(&mut self, key: Key, len: usize) -> Idx {
		self.cache_size += len;
		self.lru.insert(key)
	}

	fn remove(&mut self, index: Idx, len: usize) {
		self.lru.remove(index);
		self.cache_size -= len;
	}

	/// Decrease reference count.
	///
	/// Inserts a new entry if it reaches 0.
	///
	/// # Panics
	///
	/// If the reference count is [`RefCount::NoRef`].
	fn decrease_refcount(&mut self, refcount: &mut RefCount, key: Key, len: usize) {
		debug_assert!(
			key.flags() & Key::FLAG_OBJECT == 0
				|| key.id() == OBJECT_LIST_ID
				|| key.id() & ID_PSEUDO == 0,
			"pseudo-object don't belong in the LRU (id: {:#x})",
			key.id(),
		);
		let RefCount::Ref { count } = refcount else { panic!("NoRef") };
		match NonZeroUsize::new(count.get() - 1) {
			Some(c) => *count = c,
			None => {
				let lru_index = self.add(key, len);
				*refcount = RefCount::NoRef { lru_index };
			}
		}
	}

	/// Increase reference count.
	///
	/// Removes entry if it was 0.
	///
	/// # Panics
	///
	/// If the reference count overflows.
	fn increase_refcount(&mut self, refcount: &mut RefCount, len: usize) {
		match refcount {
			RefCount::Ref { count } => *count = count.checked_add(1).unwrap(),
			RefCount::NoRef { lru_index } => {
				self.remove(*lru_index, len);
				*refcount = RefCount::Ref { count: NonZeroUsize::new(1).unwrap() };
			}
		}
	}

	/// Decrease reference count for an entry.
	pub fn entry_decrease_refcount(&mut self, key: Key, refcount: &mut RefCount, len: usize) {
		self.decrease_refcount(refcount, key, CACHE_ENTRY_FIXED_COST + len);
	}

	/// Decrease reference count for an object.
	///
	/// If the reference count reaches zero, either
	/// - for regular objects: a new entry is added.
	/// - for pseudo-objects: `true` is returned to indicate the object should be destroyed.
	///
	/// In all other cases `false` is returned.
	pub fn object_decrease_refcount(&mut self, id: u64, refcount: &mut RefCount) -> bool {
		// Dereference the corresponding object.
		let flags = Key::FLAG_OBJECT;
		if id == OBJECT_LIST_ID || id & ID_PSEUDO == 0 {
			// Regular object.
			self.decrease_refcount(refcount, Key::new(flags, id, 0, 0), CACHE_OBJECT_FIXED_COST);
			false
		} else {
			// Pseudo-object.
			let RefCount::Ref { count } = refcount else { panic!("dangling pseudo object") };
			if let Some(c) = NonZeroUsize::new(count.get() - 1) {
				*count = c;
				false
			} else {
				true
			}
		}
	}

	/// Increase reference count for an object.
	pub fn object_increase_refcount(&mut self, refcount: &mut RefCount) {
		self.increase_refcount(refcount, CACHE_OBJECT_FIXED_COST)
	}

	/// Add an entry, depending on `refcount`.
	pub fn entry_add(&mut self, key: Key, refcount: Option<NonZeroUsize>, len: usize) -> RefCount {
		match refcount {
			Some(count) => RefCount::Ref { count },
			None => RefCount::NoRef { lru_index: self.add(key, len + CACHE_ENTRY_FIXED_COST) },
		}
	}

	/// Add an object, depending on `refcount`.
	pub fn object_add(&mut self, id: u64, refcount: Option<NonZeroUsize>) -> RefCount {
		match refcount {
			Some(count) => RefCount::Ref { count },
			None => RefCount::NoRef {
				lru_index: self.add(
					Key::new(Key::FLAG_OBJECT, id, 0, 0),
					CACHE_OBJECT_FIXED_COST,
				),
			},
		}
	}

	/// Remove an entry.
	pub fn entry_remove(&mut self, index: Idx, len: usize) {
		self.remove(index, len + CACHE_ENTRY_FIXED_COST)
	}

	/// Remove an object.
	pub fn object_remove(&mut self, index: Idx) {
		self.remove(index, CACHE_OBJECT_FIXED_COST)
	}

	/// Get the amount of bytes cached.
	pub fn size(&self) -> usize {
		self.cache_size
	}

	/// Whether there is an excess of cached data.
	pub fn has_excess(&self) -> bool {
		self.cache_size > self.cache_max
	}

	/// Set the maximum amount of cached data the LRU should keep
	/// before `Self::has_excess` returns `true`.
	pub fn set_cache_max(&mut self, size: usize) {
		self.cache_max = size
	}

	/// Get the key of the last entry.
	pub fn last(&self) -> Option<Key> {
		self.lru.last().copied()
	}

	/// Adjust the data usage of an entry.
	///
	/// This puts the entry at the *back* of the queue.
	pub fn entry_adjust(&mut self, refcount: &RefCount, old_size: usize, new_size: usize) {
		if let RefCount::NoRef { lru_index } = *refcount {
			self.cache_size += new_size;
			self.cache_size -= old_size;
			self.lru.promote(lru_index);
		}
	}

	/// Get a mutable reference to the key of an entry.
	pub fn get_mut(&mut self, index: Idx) -> Option<&mut Key> {
		self.lru.get_mut(index)
	}
}

impl<D: super::Dev, R: super::Resource> super::Cache<D, R> {
	/// Check if cache size matches real usage
	#[cfg(test)]
	#[track_caller]
	pub(crate) fn verify_cache_usage(&self) {
		use super::{Buf, Present, Slot};
		let data = self.data.borrow();
		let real_usage = data.objects.values().fold(0, |x, s| {
			let mut y = 0;
			if let Slot::Present(slot) = s {
				if matches!(slot.refcount, RefCount::NoRef { .. }) {
					y += CACHE_OBJECT_FIXED_COST;
				}
				y += slot
					.data
					.data
					.iter()
					.flat_map(|m| m.slots.values())
					.flat_map(|s| match s {
						Slot::Present(Present { data, refcount: RefCount::NoRef { .. } }) => {
							Some(data)
						}
						_ => None,
					})
					.fold(0, |x, v| x + v.len() + CACHE_ENTRY_FIXED_COST);
			}
			x + y
		});
		assert_eq!(real_usage, data.lru.size(), "cache size mismatch");
	}
}

#[derive(Debug)]
struct Node<V> {
	next: Idx,
	prev: Idx,
	value: V,
}

/// A list of LRU entries.
///
/// This is only a part of a LRU.
/// It only keeps track of nodes and their ordering based on usage.
/// Insertions and removals need to be done manually.
#[derive(Debug)]
struct LruList<V> {
	/// The most recently used node, if any.
	head: Idx,
	/// The least recently used node, if any.
	tail: Idx,
	/// Nodes in the linked list.
	///
	/// An arena is used to reduce memory fragmentation and allow the use of smaller indices
	/// compared to pointers.
	nodes: arena::Arena<Node<V>, Gen>,
}

impl<V> Default for LruList<V> {
	fn default() -> Self {
		Self { head: IDX_NONE, tail: IDX_NONE, nodes: Default::default() }
	}
}

impl<V> LruList<V> {
	/// Insert a new value at the top of the list..
	pub fn insert(&mut self, value: V) -> Idx {
		let idx = self
			.nodes
			.insert(Node { next: IDX_NONE, prev: IDX_NONE, value });
		self.push_front(idx);
		#[cfg(test)]
		self.assert_valid();
		idx
	}

	/// Promote a node to the top of the list.
	///
	/// # Panics
	///
	/// If the node at the index does not exist.
	pub fn promote(&mut self, index: Idx) {
		self.remove_list(index);
		self.push_front(index);
		#[cfg(test)]
		self.assert_valid();
	}

	/// Remove a node from the list.
	///
	/// # Panics
	///
	/// If the node at the index does not exist.
	pub fn remove(&mut self, index: Idx) -> V {
		self.remove_list(index);
		let val = self.nodes.remove(index).unwrap().value;
		#[cfg(test)]
		self.assert_valid();
		val
	}

	/// Get the last node from the list.
	pub fn last(&self) -> Option<&V> {
		(!self.nodes.is_empty()).then(|| &self.nodes.get(self.tail).unwrap().value)
	}

	/// Get a mutable reference to a value by node index.
	pub fn get_mut(&mut self, index: Idx) -> Option<&mut V> {
		self.nodes.get_mut(index).map(|node| &mut node.value)
	}

	/// Insert a value at the front of the list.
	///
	/// # Panics
	///
	/// If the node at the index does not exist.
	fn push_front(&mut self, index: Idx) {
		if self.head == IDX_NONE {
			// Make both head and tail point to index
			debug_assert!(self.tail == IDX_NONE);
			self.tail = index;
		} else {
			// Make previous head point to new head.
			debug_assert!(self.tail != IDX_NONE);
			debug_assert!(self.nodes[self.head].next == IDX_NONE);
			debug_assert!(self.nodes[self.tail].prev == IDX_NONE);

			self.nodes[self.head].next = index;
			self.nodes[index].prev = self.head;
			self.nodes[index].next = IDX_NONE;
		}
		self.head = index;
	}

	/// Remove a node from the list without removing it from the arena.
	///
	/// # Panics
	///
	/// If the node at the index does not exist.
	fn remove_list(&mut self, index: Idx) {
		let node = &self.nodes[index];
		let (prev, next) = (node.prev, node.next);

		// Link neighbours together.
		// Also remove from tail and head, if necessary.
		if prev != IDX_NONE {
			self.nodes[prev].next = next;
		} else {
			self.tail = next;
		}

		if next != IDX_NONE {
			self.nodes[next].prev = prev;
		} else {
			self.head = prev;
		}
	}

	/// Check if all nodes in the linked list are still connected.
	#[cfg(test)]
	#[track_caller]
	fn assert_valid(&self) {
		let mut index = self.tail;
		let mut prev = IDX_NONE;
		for _ in 0..self.nodes.len() {
			assert_ne!(index, IDX_NONE, "index is none before end");
			assert_eq!(self.nodes[index].prev, prev, "prev doesn't match");
			(prev, index) = (index, self.nodes[index].next);
		}
		assert_eq!(index, IDX_NONE, "cycle in list");
	}
}
