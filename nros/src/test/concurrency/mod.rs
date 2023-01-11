use {
	super::*,
	core::{future, pin::Pin},
	futures_util::{stream::FuturesUnordered, StreamExt, TryStreamExt},
};

async fn new(delay: usize) -> Nros<SlowDev, StdResource> {
	let s = SlowDev { dev: MemDev::new(1 << 16, BlockSize::K1), alloc: SlowAllocator { delay } };
	Nros::new(
		StdResource::new(),
		[[s]],
		BlockSize::K1,
		MaxRecordSize::K1,
		Compression::None,
		4096,
	)
	.await
	.unwrap()
}

/// Device with a simulated amount of delay.
struct SlowDev {
	dev: MemDev,
	alloc: SlowAllocator,
}

/// Allocator with a simulated amount of delay.
struct SlowAllocator {
	delay: usize,
}

/// Future that introduces an arbitrary delay, measured in polls.
struct SlowTask<T: Future> {
	/// The future to poll at the end of the delay.
	future: Pin<Box<T>>,
	/// How many `poll`s to wait before returning [`Poll::Ready`].
	delay: usize,
}

impl<T: Future> SlowTask<T> {
	fn new(future: T, delay: usize) -> Self {
		Self { future: Box::pin(future), delay }
	}
}

impl<T: Future> Future for SlowTask<T> {
	type Output = T::Output;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		if self.delay == 0 {
			self.future.as_mut().poll(cx)
		} else {
			self.delay -= 1;
			// Necessary because Rust's generated tasks are quite smart apparently.
			cx.waker().wake_by_ref();
			Poll::Pending
		}
	}
}

impl Dev for SlowDev {
	type Allocator = SlowAllocator;
	type Error = <MemDev as Dev>::Error;
	type ReadTask<'a> = SlowTask<<MemDev as Dev>::ReadTask<'a>>;
	type WriteTask<'a> = SlowTask<<MemDev as Dev>::WriteTask<'a>>;
	type FenceTask<'a> = SlowTask<<MemDev as Dev>::FenceTask<'a>>;

	fn block_count(&self) -> u64 {
		self.dev.block_count()
	}

	fn block_size(&self) -> BlockSize {
		self.dev.block_size()
	}

	fn read(&self, lba: u64, len: usize) -> Self::ReadTask<'_> {
		SlowTask::new(self.dev.read(lba, len), self.alloc.delay)
	}

	fn write(
		&self,
		lba: u64,
		buf: <<MemDev as Dev>::Allocator as Allocator>::Buf,
	) -> Self::WriteTask<'_> {
		SlowTask::new(self.dev.write(lba, buf), self.alloc.delay)
	}

	fn fence(&self) -> Self::FenceTask<'_> {
		SlowTask::new(self.dev.fence(), self.alloc.delay)
	}

	fn allocator(&self) -> &Self::Allocator {
		&self.alloc
	}
}

impl Allocator for SlowAllocator {
	type Buf = <MemAllocator as Allocator>::Buf;
	type Error = <MemAllocator as Allocator>::Error;
	type AllocTask<'a> = SlowTask<<MemAllocator as Allocator>::AllocTask<'a>>;

	fn alloc(&self, size: usize) -> Self::AllocTask<'_> {
		SlowTask::new(MemAllocator.alloc(size), self.delay)
	}
}

/// Read from an object concurrently.
#[test]
fn read() {
	// FIXME there is a source of non-determinism somewhere
	// and it's pissing me off.
	let s = block_on(new(5));
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		// Write to at least 8 different leaves to ensure we exceed cache limits
		obj.resize(1024 * 8).await.unwrap();
		for i in 0..8 {
			obj.write(i * 1024 + 1023, &[1]).await.unwrap();
		}

		// Read concurrently.
		let obj = &obj;
		(0..8)
			.map(|i| async move {
				let buf = &mut [0];
				obj.read(1024 * i + 1023, buf).await.unwrap();
				assert_eq!(*buf, [1]);
			})
			.collect::<FuturesUnordered<_>>()
			.for_each(|()| future::ready(()))
			.await;
	});
	block_on(bg.drop()).unwrap();
}

/// Write to an object concurrently.
#[test]
fn write() {
	let s = block_on(new(5));
	let bg = Background::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		// Write concurrently.
		// Write to at least 8 different leaves to ensure we exceed cache limits
		obj.resize(1024 * 8).await.unwrap();
		(0..8)
			.map(|i| obj.write(1024 * i + 1023, &[1]))
			.collect::<FuturesUnordered<_>>()
			.try_for_each(|_| future::ready(Ok(())))
			.await
			.unwrap();

		for i in 0..8 {
			let buf = &mut [0];
			obj.read(1024 * i + 1023, buf).await.unwrap();
			assert_eq!(*buf, [1]);
		}
	});
	block_on(bg.drop()).unwrap();
}
