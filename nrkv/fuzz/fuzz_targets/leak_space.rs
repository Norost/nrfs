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

const MAX: u8 = 8;

#[derive(Debug)]
enum Op {
	Alloc { len: u8 },
	Dealloc { i: u8 },
	InsertItem,
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
		Some(match *b >> 6 {
			0 => Op::Alloc { len: *b % MAX },
			1 => Op::InsertItem,
			2 | 3 => Op::Dealloc { i: *b % MAX },
			_ => unreachable!(),
		})
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
		let buf = &mut vec![0; (64 + 6 * 4096) + (16 * 64)][..];
		let kv = &mut nrkv::Nrkv::init_with_key(buf, nrkv::StaticConf::<0, 0>, [0; 16])
			.await
			.unwrap();
		enum E {
			A(nrkv::Tag, u8),
			I(nrkv::Tag),
		}
		let mut alloc = vec![];
		let buf = &mut [0; 255];
		for op in &ops {
			match op {
				Op::Alloc { len } => {
					if alloc.len() < usize::from(MAX) {
						let offt = kv.alloc(len.into()).await.unwrap();
						kv.read(offt.get(), &mut buf[..len.into()]).await.unwrap();
						assert!(buf.iter().all(|&b| b == 0), "{:?}", &buf[..len.into()]);
						kv.write(offt.get(), &[]).await.unwrap();
						alloc.push(E::A(offt, len));
					}
				}
				Op::InsertItem => {
					if alloc.len() < usize::from(MAX) {
						if let Ok(tag) = kv.insert(b"\0".into(), &[]).await.unwrap() {
							alloc.push(E::I(tag));
						}
					}
				}
				Op::Dealloc { i } => {
					if !alloc.is_empty() {
						match alloc.swap_remove(usize::from(i) % alloc.len()) {
							E::A(offt, len) => {
								kv.dealloc(offt.get(), len.into()).await.unwrap();
							}
							E::I(tag) => {
								kv.remove(tag).await.unwrap();
							}
						}
					}
				}
			}
		}
	})
});
