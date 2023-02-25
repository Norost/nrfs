#[cfg(debug_assertions)]
pub type Gen = u8;
#[cfg(not(debug_assertions))]
pub type Gen = ();

pub type Idx = arena::Handle<Gen>;

#[cfg(debug_assertions)]
pub const IDX_NONE: Idx = Idx::from_raw(usize::MAX, u8::MAX);
#[cfg(not(debug_assertions))]
pub const IDX_NONE: Idx = Idx::from_raw(usize::MAX, ());

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
pub struct LruList<V> {
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

	/// Get the amount of live nodes.
	pub fn len(&self) -> usize {
		self.nodes.len()
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
