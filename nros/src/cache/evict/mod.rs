mod entry;

use {
	super::Cache,
	crate::{Background, Dev, Resource},
};

impl<D: Dev, R: Resource> Cache<D, R> {
	/// Evict entries if global cache limits are being exceeded.
	///
	/// This is a background task and does not finish.
	pub(super) async fn evict_excess<'a: 'b, 'b>(&'a self, bg: &'b Background<'a, D>) -> ! {
		let task = crate::waker_queue::poll(move |cx| loop {
			let key = match self.data().mem.evict_next(cx.waker()) {
				Ok(k) => k,
				Err(t) => break Err(t),
			};
			let task = self.evict_entry(key);
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
