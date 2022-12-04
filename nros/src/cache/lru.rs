pub type Idx = arena::Handle<u8>;

const IDX_NONE: Idx = Idx::from_raw(usize::MAX, u8::MAX);

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
	nodes: arena::Arena<Node<V>, u8>,
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
		idx
	}

	/// Promote a node to the top of the list.
	///
	/// # Panics
	///
	/// If the node at the index does not exist.
	pub fn promote(&mut self, index: Idx) {
		self.remove_list(index);
		self.push_front(index)
	}

	/// Remove a node from the list.
	///
	/// # Panics
	///
	/// If the node at the index does not exist.
	pub fn remove(&mut self, index: Idx) -> V {
		self.remove_list(index);
		self.nodes.remove(index).unwrap().value
	}

	/// Remove the last node from the list.
	pub fn remove_last(&mut self) -> Option<V> {
		(!self.nodes.is_empty()).then(|| {
			let idx = self.pop_last();
			self.nodes.remove(idx).unwrap().value
		})
	}

	/// Get a value by node index.
	pub fn get(&self, index: Idx) -> Option<&V> {
		self.nodes.get(index).map(|node| &node.value)
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
			self.nodes[self.head].next = index;
			self.nodes[index].prev = self.head;
		}
		self.head = index;
	}

	/// Remove the last value from the list.
	///
	/// # Panics
	///
	/// If there are no nodes.
	fn pop_last(&mut self) -> Idx {
		let tail = self.tail;
		self.remove_list(tail);
		tail
	}

	/// Remove a node from the list without removing it from the arena.
	///
	/// # Panics
	///
	/// If the node at the index does not exist.
	fn remove_list(&mut self, index: Idx) {
		let prev = self.nodes[index].prev;
		let next = self.nodes[index].next;

		// Remove from head and tail
		if index == self.tail {
			self.tail = next;
			if next != IDX_NONE {
				self.nodes[next].prev = IDX_NONE;
			}
		}
		if index == self.head {
			self.head = prev;
			if prev != IDX_NONE {
				self.nodes[prev].next = IDX_NONE;
			}
		}
	}
}
