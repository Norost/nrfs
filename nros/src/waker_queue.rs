use {
	alloc::rc::{Rc, Weak},
	core::{
		cell::Cell,
		fmt, future,
		task::{Context, Poll, Waker},
	},
};

/// Queue of wakers.
#[derive(Default)]
pub struct WakerQueue<V> {
	head: Option<Rc<Node<V>>>,
	tail: Weak<Node<V>>,
}

impl<V: fmt::Debug> fmt::Debug for WakerQueue<V> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut f = f.debug_list();
		let mut node = self.head.clone();
		while let Some(n) = node {
			f.entry(&n);
			node = n.next.take();
			n.next.set(node.clone());
		}
		f.finish()
	}
}

pub struct Node<V> {
	waker: Cell<Option<Waker>>,
	value: V,
	next: Cell<Option<Rc<Node<V>>>>,
}

impl<V: fmt::Debug> fmt::Debug for Node<V> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut f = f.debug_struct(stringify!(Node));

		let waker = self.waker.take();
		f.field("waker", waker.as_ref().expect("node without waker"));
		self.waker.set(waker);

		if core::mem::size_of_val(&self.value) > 0 {
			f.field("value", &self.value);
		}

		f.finish()
	}
}

impl<V> WakerQueue<V> {
	#[must_use]
	pub fn push(&mut self, waker: Waker, value: V) -> WakerQueueTicket<V> {
		let node = Rc::new(Node { waker: Cell::new(Some(waker)), value, next: None.into() });
		let weak = Rc::downgrade(&node);

		if let Some(tail) = self.tail.upgrade() {
			tail.next.set(Some(node));
		} else {
			self.head = Some(node);
		}
		self.tail = weak.clone();

		WakerQueueTicket { node: weak }
	}

	/// Get node with next task to wake.
	pub fn get_next(&self) -> Option<&Node<V>> {
		self.head.as_deref()
	}

	pub fn wake_next(&mut self) -> Option<V> {
		trace!("wake_next");
		let head = self.head.take()?;
		let head = Rc::try_unwrap(head).unwrap_or_else(|_| panic!("more than one strong ref"));

		head.waker.take().map(|w| w.wake());
		self.head = head.next.take();

		trace!(info "woke one");

		Some(head.value)
	}

	pub fn wake_all(&mut self) {
		trace!("wake_all");

		let mut count = 0;
		while let Some(head) = self.head.take() {
			let head = Rc::try_unwrap(head).unwrap_or_else(|_| panic!("more than one strong ref"));
			head.waker.take().map(|w| w.wake());
			self.head = head.next.take();
			count += 1;
		}

		trace!(info "woke {}", count);
	}
}

/// Manual drop to avoid stack overflow.
impl<V> Drop for WakerQueue<V> {
	fn drop(&mut self) {
		while let Some(head) = self.head.take() {
			self.head = head.next.take();
		}
	}
}

#[derive(Debug)]
pub struct WakerQueueTicket<V> {
	node: Weak<Node<V>>,
}

impl<V> WakerQueueTicket<V> {
	/// Returns `false` on failure.
	#[must_use = "node may no longer be in queue"]
	fn set_waker(&mut self, waker: &Waker) -> bool {
		if let Some(node) = self.node.upgrade() {
			node.waker.set(Some(waker.clone()));
			true
		} else {
			false
		}
	}
}

impl<V> Default for WakerQueueTicket<V> {
	fn default() -> Self {
		Self { node: Default::default() }
	}
}

pub async fn poll<R, V, F>(mut f: F) -> R
where
	F: FnMut(&mut Context<'_>) -> Result<R, WakerQueueTicket<V>>,
{
	let mut ticket = WakerQueueTicket::default();
	future::poll_fn(move |cx| {
		if ticket.set_waker(cx.waker()) {
			return Poll::Pending;
		}
		match f(cx) {
			Ok(r) => Poll::Ready(r),
			Err(t) => {
				ticket = t;
				Poll::Pending
			}
		}
	})
	.await
}

#[cfg(test)]
mod test {
	use {super::*, futures_util::task::noop_waker};

	#[test]
	fn push_set() {
		let mut q = WakerQueue::default();
		let mut t = q.push(noop_waker(), ());
		assert!(t.set_waker(&noop_waker()));
	}

	#[test]
	fn push_wake() {
		let mut q = WakerQueue::default();
		let _ = q.push(noop_waker(), ());
		q.wake_next().unwrap();
	}
}
