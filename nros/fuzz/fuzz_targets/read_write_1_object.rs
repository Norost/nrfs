#![no_main]
#![feature(pin_macro)]

use libfuzzer_sys::fuzz_target;

use nros::*;
use core::future::Future;
use core::task::Context;

async fn new(max_record_size: MaxRecordSize) -> Nros<MemDev> {
	let s = MemDev::new(16, BlockSize::K1);
	Nros::new(
		[[s]],
		BlockSize::K1,
		max_record_size,
		Compression::None,
		// Don't evict cache for tests with small amounts of data, effectively.
		4 * 1024,
		4 * 1024,
	)
	.await
	.unwrap()
}

// Macro hygiene please!
fn run_<F, Fut>(f: F)
where
	F: Fn() -> Fut,
	Fut: Future<Output = ()>,
{
	let mut fut = core::pin::pin!(f());
	let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
	while fut.as_mut().poll(&mut cx).is_pending() {}
}

fuzz_target!(|data: &[u8]| {
    run_(|| async {
        let s = new(MaxRecordSize::K1).await;
        let obj = s.create().await.unwrap();
        obj.resize(data.len().try_into().unwrap()).await.unwrap();
        
        obj.write(0, data).await.unwrap();
        
        let mut buf = vec![0; data.len()];
        obj.read(0, &mut buf).await.unwrap();

        assert_eq!(data, &buf);
    });
});
