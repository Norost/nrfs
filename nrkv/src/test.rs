use {
	crate::Nrkv,
	alloc::vec,
	core::{
		future::Future,
		task::{Context, RawWaker, RawWakerVTable, Waker},
	},
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

fn mkstore() -> alloc::vec::Vec<u8> {
	vec![0; 1 << 17]
}

#[test]
fn init() {
	run(async {
		let _ = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
	});
}

#[test]
fn load() {
	run(async {
		let kv = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
		let sto = kv.save().await.unwrap();
		let _ = Nrkv::load(sto).await.unwrap();
	});
}

#[test]
fn insert_one() {
	run(async {
		let mut kv = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
		kv.insert(b"hello".into(), &[]).await.unwrap().unwrap();
	});
}

#[test]
fn find_one() {
	run(async {
		let mut kv = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
		kv.insert(b"hello".into(), &[]).await.unwrap().unwrap();
		kv.find(b"hello".into()).await.unwrap().unwrap();
	});
}

#[test]
fn find_none() {
	run(async {
		let mut kv = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
		kv.insert(b"hello".into(), &[]).await.unwrap().unwrap();
		assert!(kv.find(b"quack".into()).await.unwrap().is_none());
	});
}

#[test]
fn insert_dup() {
	run(async {
		let mut kv = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
		kv.insert(b"hello".into(), &[]).await.unwrap().unwrap();
		assert!(kv.insert(b"hello".into(), &[]).await.unwrap().is_none());
	});
}

#[test]
fn insert_collide() {
	run(async {
		let mut kv = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
		kv.insert((&[17, 4]).into(), &[]).await.unwrap().unwrap();
		kv.insert(b"RV".into(), &[]).await.unwrap().unwrap();
	});
}

#[test]
fn insert_collide3() {
	run(async {
		let mut kv = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
		kv.insert((&[17, 4]).into(), &[]).await.unwrap().unwrap();
		kv.insert(b"RV".into(), &[]).await.unwrap().unwrap();
		kv.insert((&[167, 114]).into(), &[]).await.unwrap().unwrap();
	});
}

#[test]
fn insert_collide_dup() {
	run(async {
		let mut kv = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
		kv.insert((&[17, 4]).into(), &[]).await.unwrap().unwrap();
		kv.insert(b"RV".into(), &[]).await.unwrap().unwrap();
		assert!(kv.insert(b"RV".into(), &[]).await.unwrap().is_none());
	});
}

#[test]
fn find_collide() {
	run(async {
		let mut kv = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
		kv.insert((&[17, 4]).into(), &[]).await.unwrap().unwrap();
		kv.insert(b"RV".into(), &[]).await.unwrap().unwrap();
		kv.find((&[17, 4]).into()).await.unwrap().unwrap();
		kv.find(b"RV".into()).await.unwrap().unwrap();
	});
}

#[test]
fn remove() {
	run(async {
		let mut kv = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
		kv.insert((&[17, 4]).into(), &[]).await.unwrap().unwrap();
		kv.insert(b"RV".into(), &[]).await.unwrap().unwrap();
		assert!(kv.remove((&[17, 4]).into()).await.unwrap());
		assert!(kv.find((&[17, 4]).into()).await.unwrap().is_none());
		assert!(!kv.remove((&[17, 4]).into()).await.unwrap());
		assert!(!kv.remove(b"AA".into()).await.unwrap());
		assert!(kv.remove(b"RV".into()).await.unwrap());
	});
}

#[test]
fn next_batch() {
	run(async {
		let mut kv = Nrkv::init(mkstore(), &mut ZeroRand, 32).await.unwrap();
		let mut tags = [
			Some(kv.insert((&[17, 4]).into(), &[]).await.unwrap().unwrap()),
			Some(kv.insert(b"RV".into(), &[]).await.unwrap().unwrap()),
			Some(kv.insert((&[167, 114]).into(), &[]).await.unwrap().unwrap()),
			Some(kv.insert(b"hi".into(), &[]).await.unwrap().unwrap()),
		];
		let mut it = Default::default();
		kv.next_batch(&mut it, &mut |tag| {
			dbg!(tag);
			for t in tags.iter_mut() {
				if *t == Some(tag) {
					*t = None;
					return true;
				}
			}
			panic!("tag not in tags");
		})
		.await
		.unwrap();
		assert!(
			tags.iter().all(|t| t.is_none()),
			"not all items visited {:?}",
			tags
		);
	});
}

#[test]
fn user_data() {
	run(async {
		let mut kv = Nrkv::init(mkstore(), &mut ZeroRand, 16).await.unwrap();
		let tag = kv.insert(b"hello".into(), &[]).await.unwrap().unwrap();
		kv.write_user_data(tag, 4, b"I'm a sheep").await.unwrap();
		let mut buf = [0; 16];
		kv.read_user_data(tag, 0, &mut buf).await.unwrap();
		assert_eq!(buf, *b"\0\0\0\0I'm a sheep\0");
	});
}
