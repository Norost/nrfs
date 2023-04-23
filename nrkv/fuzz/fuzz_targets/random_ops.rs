#![no_main]
#![feature(const_waker)]

use {
	arbitrary::Arbitrary,
	core::{
		future::Future,
		task::{Context, RawWaker, RawWakerVTable, Waker},
	},
	libfuzzer_sys::fuzz_target,
	nrkv::{Key, Nrkv, ShareNrkv},
};

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

struct ZeroRand;

impl rand_core::RngCore for ZeroRand {
	fn next_u32(&mut self) -> u32 {
		0
	}
	fn next_u64(&mut self) -> u64 {
		0
	}
	fn fill_bytes(&mut self, bytes: &mut [u8]) {
		bytes.fill(0)
	}
	fn try_fill_bytes(&mut self, bytes: &mut [u8]) -> Result<(), rand_core::Error> {
		bytes.fill(0);
		Ok(())
	}
}

impl rand_core::CryptoRng for ZeroRand {}

#[derive(Debug, Arbitrary)]
pub struct Test<'a> {
	ops: Vec<Op<'a>>,
}

#[derive(Clone, Debug, Arbitrary)]
pub enum Op<'a> {
	Add { key: &'a Key, data: [u8; 16] },
	Find { key: &'a Key },
	Remove { key: &'a Key },
	Get { tag_idx: u16 },
	Set { tag_idx: u16, data: [u8; 16] },
	Iter { break_at: u16 },
	Reload,
}

impl<'a> Test<'a> {
	async fn run(self) {
		let mut kv = Nrkv::init(vec![0; 1 << 20], &mut ZeroRand, 16)
			.await
			.unwrap();
		let mut map = std::collections::BTreeMap::new();
		let mut dat = std::collections::BTreeMap::new();
		let mut tags = vec![];
		for op in self.ops {
			match op {
				Op::Add { key, data } => {
					if let Some(tag) = kv.insert(key, &data).await.unwrap() {
						let prev = map.insert(key, tag);
						assert!(prev.is_none(), "key was already present");
						let prev = dat.insert(tag, data);
						assert!(prev.is_none(), "tag reused");
						tags.push(tag);
					} else {
						assert!(map.contains_key(key), "key isn't present");
					}
				}
				Op::Find { key } => {
					if let Some(tag) = kv.find(key).await.unwrap() {
						let &t = map.get(key).expect("key not found");
						assert_eq!(t, tag);
					} else {
						assert!(!map.contains_key(key), "key is present");
					}
				}
				Op::Remove { key } => {
					if kv.remove(key).await.unwrap() {
						let prev = map.remove(key);
						assert!(prev.is_some(), "key wasn't present");
					} else {
						assert!(!map.contains_key(key), "key is present");
					}
				}
				Op::Get { tag_idx } => {
					if tags.is_empty() {
						continue;
					}
					let tag = tags[usize::from(tag_idx) % tags.len()];
					let mut buf = [0; 16];
					kv.read_user_data(tag, 0, &mut buf).await.unwrap();
					assert_eq!(*dat.get(&tag).expect("invalid tag"), buf);
				}
				Op::Set { tag_idx, data } => {
					if tags.is_empty() {
						continue;
					}
					let tag = tags[usize::from(tag_idx) % tags.len()];
					kv.write_user_data(tag, 0, &data).await.unwrap();
					*dat.get_mut(&tag).expect("invalid tag") = data;
				}
				Op::Iter { break_at } => {
					let live = &core::cell::RefCell::new(
						map.values()
							.copied()
							.collect::<std::collections::HashSet<_>>(),
					);
					let mut state = Default::default();
					let mut i = 0;
					let kv = &ShareNrkv::new(&mut kv);
					kv.next_batch(&mut state, |tag| async move {
						let rem = live.borrow_mut().remove(&tag);
						let valid = kv.borrow_mut().read_key(tag, &mut []).await? > 0;
						assert!(rem == valid, "invalid tag or tag already removed");
						i += 1;
						Ok(i <= break_at)
					})
					.await
					.unwrap();
					kv.next_batch(&mut state, |tag| async move {
						let rem = live.borrow_mut().remove(&tag);
						let valid = kv.borrow_mut().read_key(tag, &mut []).await? > 0;
						assert!(rem == valid, "invalid tag or tag already removed");
						Ok(true)
					})
					.await
					.unwrap();
					assert!(live.borrow().is_empty(), "not all live tags visited");
				}
				Op::Reload => {
					let store = kv.save().await.unwrap();
					kv = Nrkv::load(store).await.unwrap();
				}
			}
		}
	}
}

fuzz_target!(|test: Test| {
	run(test.run());
});
