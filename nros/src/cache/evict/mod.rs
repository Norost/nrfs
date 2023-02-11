mod entry;
mod object;

use {
	super::{Cache, Key},
	crate::{Background, Dev, Resource},
};

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Evict entries if global cache limits are being exceeded.
	///
	/// This is a background task and does not finish.
	pub(super) async fn evict_excess<'a: 'b, 'b>(&'a self, bg: &'b Background<'a, D>) -> ! {
		let task = crate::waker_queue::poll(move |cx| loop {
			let (key, _) = match self
				.data
				.borrow_mut()
				.memory_tracker
				.evict_next(self.max_record_size(), cx.waker())
			{
				Ok(k) => k,
				Err(t) => break Err(t),
			};
			let task = if key.test_flag(Key::FLAG_OBJECT) {
				self.evict_object(key.id())
			} else {
				self.evict_entry(key)
			};
			if let Some(task) = task {
				// Push to background to process in parallel
				bg.add(task);
			}
		});
		#[cfg(feature = "trace")]
		let task = crate::trace::TracedTask::new(task);
		task.await
	}
}
