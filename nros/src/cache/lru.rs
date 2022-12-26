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

	/// Remove the last node from the list.
	pub fn remove_last(&mut self) -> Option<V> {
		(!self.nodes.is_empty()).then(|| {
			let idx = self.pop_last();
			let val = self.nodes.remove(idx).unwrap().value;
			#[cfg(test)]
			self.assert_valid();
			val
		})
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
