use {core::hash::Hash, rustc_hash::FxHashMap};

#[derive(Debug)]
pub struct Lru<K, V> {
	nodes: Box<[Node<K, V>]>,
	map: FxHashMap<K, u16>,
	head: u16,
	tail: u16,
	free: u16,
}

#[derive(Debug)]
struct Node<K, V> {
	key_value: Option<(K, V)>,
	next: u16,
	prev: u16,
}

impl<K, V> Lru<K, V>
where
	K: Clone + Hash + Eq,
{
	pub fn new(max: u16) -> Self {
		Self {
			nodes: (0..max)
				.map(|i| Node { key_value: None, prev: u16::MAX, next: i + 1 })
				.collect(),
			free: 0,
			map: Default::default(),
			head: u16::MAX,
			tail: u16::MAX,
		}
	}

	pub fn insert(&mut self, key: K, value: V) {
		let i;
		if let Some(v) = self.map.get(&key) {
			i = *v;
			self.remove_node(i);
		} else if self.free < self.cap() {
			i = self.free;
			self.free = self.nodes[usize::from(i)].next;
			self.map.insert(key.clone(), i);
		} else {
			i = self.tail;
			let n = &self.nodes[usize::from(i)];
			let nn = n.next;
			self.map.remove(&n.key_value.as_ref().unwrap().0);
			self.remove_node(i);
			self.map.insert(key.clone(), i);
			self.tail = nn;
		};
		self.node(i).key_value = Some((key, value));
		self.push_node(i);
		self.head = i;
	}

	pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
		let i = *self.map.get(key)?;
		self.remove_node(i);
		self.push_node(i);
		Some(&mut self.node(i).key_value.as_mut().unwrap().1)
	}

	#[cfg(test)]
	pub fn get(&mut self, key: &K) -> Option<&V> {
		self.get_mut(key).map(|v| &*v)
	}

	/// # Note
	///
	/// Does not update LRU and only serves as a workaround for borrow issues.
	pub fn contains_key(&self, key: &K) -> bool {
		self.map.contains_key(key)
	}

	pub fn remove(&mut self, key: &K) -> Option<V> {
		let i = self.map.remove(key)?;
		self.remove_node(i);
		let (_, v) = self.node(i).key_value.take().unwrap();
		self.node(i).next = self.free;
		self.free = i;
		Some(v)
	}

	fn cap(&self) -> u16 {
		self.nodes.len() as _
	}

	fn push_node(&mut self, i: u16) {
		self.node(i).next = u16::MAX;
		self.node(i).prev = self.head;
		if self.head < self.cap() {
			self.node(self.head).next = i;
		}
		self.head = i;
		if self.tail >= self.cap() {
			self.tail = i;
		}
		debug_assert!(self.head < self.cap());
		debug_assert!(self.tail < self.cap());
	}

	fn remove_node(&mut self, i: u16) {
		let cap = self.cap();
		let (n, p) = (self.node(i).next, self.node(i).prev);
		(n < cap).then(|| self.node(n).prev = p);
		(p < cap).then(|| self.node(p).next = n);
		(self.head == i).then(|| self.head = p);
		(self.tail == i).then(|| self.tail = n);
	}

	#[track_caller]
	fn node(&mut self, i: u16) -> &mut Node<K, V> {
		&mut self.nodes[usize::from(i)]
	}
}

#[cfg(test)]
mod test {
	use super::*;

	fn fuzz(ki: usize, li: usize, km: usize, lm: usize) {
		let mut lru = Lru::new(4);
		let mut k @ mut l = 0;
		for _ in 0..10_000 {
			let i = k % km;
			match l % lm {
				0 => {
					lru.insert(i, i);
				}
				1 => {
					lru.get(&i);
				}
				2 => {
					lru.remove(&i);
				}
				_ => {}
			}
			k += ki;
			l += li;
		}
	}

	#[test]
	fn insert() {
		let mut lru = Lru::new(4);
		lru.insert(0, 0);
		lru.insert(1, 1);
		lru.insert(2, 2);
		lru.insert(3, 3);
		lru.insert(4, 4);
		assert_eq!(lru.get(&0).copied(), None);
		assert_eq!(lru.get(&1).copied(), Some(1));
		assert_eq!(lru.get(&2).copied(), Some(2));
		assert_eq!(lru.get(&3).copied(), Some(3));
		assert_eq!(lru.get(&4).copied(), Some(4));
	}

	#[test]
	fn get() {
		let mut lru = Lru::new(4);
		lru.insert(0, 0);
		lru.insert(1, 1);
		lru.insert(2, 2);
		lru.get(&0);
		lru.insert(3, 3);
		lru.insert(4, 4);
		lru.get(&4);
		assert_eq!(lru.get(&0).copied(), Some(0));
		assert_eq!(lru.get(&1).copied(), None);
		assert_eq!(lru.get(&2).copied(), Some(2));
		assert_eq!(lru.get(&3).copied(), Some(3));
		assert_eq!(lru.get(&4).copied(), Some(4));
	}

	#[test]
	fn remove() {
		let mut lru = Lru::new(4);
		lru.insert(0, 0);
		lru.insert(1, 1);
		lru.insert(2, 2);
		lru.insert(3, 3);
		lru.remove(&2);
		lru.insert(4, 4);
		assert_eq!(lru.get(&0).copied(), Some(0));
		assert_eq!(lru.get(&1).copied(), Some(1));
		assert_eq!(lru.get(&2).copied(), None);
		assert_eq!(lru.get(&3).copied(), Some(3));
		assert_eq!(lru.get(&4).copied(), Some(4));
	}

	#[test]
	fn remove_head() {
		let mut lru = Lru::new(4);
		lru.insert(0, 0);
		lru.insert(1, 1);
		lru.insert(2, 2);
		lru.insert(3, 3);
		lru.remove(&3);
		lru.insert(4, 4);
		assert_eq!(lru.get(&0).copied(), Some(0));
		assert_eq!(lru.get(&1).copied(), Some(1));
		assert_eq!(lru.get(&2).copied(), Some(2));
		assert_eq!(lru.get(&3).copied(), None);
		assert_eq!(lru.get(&4).copied(), Some(4));
	}

	#[test]
	fn remove_tail() {
		let mut lru = Lru::new(4);
		lru.insert(0, 0);
		lru.insert(1, 1);
		lru.insert(2, 2);
		lru.insert(3, 3);
		lru.remove(&0);
		lru.insert(4, 4);
		assert_eq!(lru.get(&0).copied(), None);
		assert_eq!(lru.get(&1).copied(), Some(1));
		assert_eq!(lru.get(&2).copied(), Some(2));
		assert_eq!(lru.get(&3).copied(), Some(3));
		assert_eq!(lru.get(&4).copied(), Some(4));
	}

	#[test]
	fn fuzz_a() {
		fuzz(13, 7, 7, 3);
	}

	#[test]
	fn nrfs_real_case_fail_000_minified() {
		let mut lru = Lru::new(32);
		lru.insert(1, ());
		lru.insert(0, ());
		lru.insert(2, ());
		lru.remove(&2);
		lru.insert(5, ());
		lru.insert(7, ());
		lru.remove(&0);
		lru.insert(15, ());
		lru.insert(14, ());
		lru.remove(&14);
		lru.insert(0, ());
	}
}
