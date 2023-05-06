#![no_main]
#![feature(const_waker)]

use {
	libfuzzer_sys::{arbitrary, fuzz_target},
	std::{
		fmt,
		future::Future,
		task::{Context, RawWaker, RawWakerVTable, Waker},
	},
};

#[derive(Debug)]
enum Op {
	Alloc { len: u8 },
	Dealloc { i: u8 },
}

#[derive(arbitrary::Arbitrary)]
struct Ops<'a>(&'a [u8]);

impl<'a> IntoIterator for &Ops<'a> {
	type Item = Op;
	type IntoIter = OpsIter<'a>;

	fn into_iter(self) -> Self::IntoIter {
		OpsIter(self.0)
	}
}

impl fmt::Debug for Ops<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut f = f.debug_list();
		for b in self.into_iter() {
			f.entry(&b);
		}
		f.finish()
	}
}

struct OpsIter<'a>(&'a [u8]);

impl Iterator for OpsIter<'_> {
	type Item = Op;

	fn next(&mut self) -> Option<Self::Item> {
		let b;
		(b, self.0) = self.0.split_first()?;
		if *b & 0x80 == 0 {
			Some(Op::Alloc { len: *b % 32 })
		} else {
			Some(Op::Dealloc { i: *b % 32 })
		}
	}
}

static NOOP_WAKER: Waker = {
	const VTBL: RawWakerVTable = RawWakerVTable::new(|_| RAW_WAKER, |_| (), |_| (), |_| ());
	const RAW_WAKER: RawWaker = RawWaker::new(1 as _, &VTBL);
	unsafe { Waker::from_raw(RAW_WAKER) }
};

fn run(f: impl Future<Output = ()>) {
	let mut f = core::pin::pin!(f);
	let mut cx = Context::from_waker(&NOOP_WAKER);
	for _ in 0..100 {
		if f.as_mut().poll(&mut cx).is_ready() {
			return;
		}
	}
	panic!("stuck");
}

fuzz_target!(|ops: Ops<'_>| {
	run(async move {
		let buf = &mut [0; (64 + 6 * 4096) + (32 * 128)][..];
		let kv = &mut nrkv::Nrkv::init_with_key(buf, nrkv::StaticConf::<0, 0>, [0; 16])
			.await
			.unwrap();
		let mut alloc = vec![];
		let buf = &mut [0; 255];
		for op in &ops {
			match op {
				Op::Alloc { len } => {
					if alloc.len() < 32 {
						let offt = kv.alloc(len.into()).await.unwrap();
						kv.read(offt.get(), &mut buf[..len.into()]).await.unwrap();
						assert!(buf.iter().all(|&b| b == 0), "{:?}", &buf[..len.into()]);
						kv.write(offt.get(), &[]).await.unwrap();
						alloc.push((offt, len));
					}
				}
				Op::Dealloc { i } => {
					if !alloc.is_empty() {
						let (offt, len) = alloc.swap_remove(usize::from(i) % alloc.len());
						kv.dealloc(offt.get(), len.into()).await.unwrap();
					}
				}
			}
		}
	})
});
