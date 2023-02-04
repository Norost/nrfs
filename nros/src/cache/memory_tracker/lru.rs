use super::super::Key;

#[cfg(debug_assertions)]
pub type Gen = u8;
#[cfg(not(debug_assertions))]
pub type Gen = ();

pub type Idx = arena::Handle<Gen>;

#[cfg(debug_assertions)]
const IDX_NONE: Idx = Idx::from_raw(usize::MAX, u8::MAX);
#[cfg(not(debug_assertions))]
const IDX_NONE: Idx = Idx::from_raw(usize::MAX, ());

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

	/// Add a new entry to the LRU.
	pub fn add(&mut self, key: Key, len: usize) -> Idx {
		self.cache_size += len;
		self.lru.insert(key)
	}

	pub fn remove(&mut self, index: Idx, len: usize) -> Key {
		self.cache_size -= len;
		self.lru.remove(index)
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

	/// Get the key and handle of the last entry.
	pub fn last(&self) -> Option<(Key, Idx)> {
		self.lru.last().map(|(k, &v)| (v, k))
	}

	/// Adjust the data usage of a node.
	pub fn adjust(&mut self, old_size: usize, new_size: usize) {
		self.cache_size += new_size;
		self.cache_size -= old_size;
	}

	/// Move an entry to the back of the queue.
	pub fn touch(&mut self, index: Idx) {
		self.lru.promote(index);
	}

	/// Get a mutable reference to the key of an entry.
	pub fn get_mut(&mut self, index: Idx) -> Option<&mut Key> {
		self.lru.get_mut(index)
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

	/// Get the value & handle last node from the list.
	pub fn last(&self) -> Option<(Idx, &V)> {
		(!self.nodes.is_empty()).then(|| {
			let node = self.nodes.get(self.tail).unwrap();
			(self.tail, &node.value)
		})
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
