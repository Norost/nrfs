use {
	crate::{Nrkv, ShareNrkv},
	alloc::vec,
	core::{
		cell::{Cell, RefCell},
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

fn mkstore() -> alloc::vec::Vec<u8> {
	vec![0; 1 << 17]
}

async fn mkkv() -> Nrkv<Vec<u8>> {
	Nrkv::init_with_key(mkstore(), [0; 16], 32).await.unwrap()
}

#[test]
fn load() {
	run(async {
		let kv = mkkv().await;
		let sto = kv.save().await.unwrap();
		let _ = Nrkv::load(sto).await.unwrap();
	});
}

#[test]
fn insert_one() {
	run(async {
		let mut kv = mkkv().await;
		kv.insert(b"hello".into(), &[]).await.unwrap().unwrap();
	});
}

#[test]
fn find_one() {
	run(async {
		let mut kv = mkkv().await;
		kv.insert(b"hello".into(), &[]).await.unwrap().unwrap();
		kv.find(b"hello".into()).await.unwrap().unwrap();
	});
}

#[test]
fn find_none() {
	run(async {
		let mut kv = mkkv().await;
		kv.insert(b"hello".into(), &[]).await.unwrap().unwrap();
		assert!(kv.find(b"quack".into()).await.unwrap().is_none());
	});
}

#[test]
fn insert_dup() {
	run(async {
		let mut kv = mkkv().await;
		kv.insert(b"hello".into(), &[]).await.unwrap().unwrap();
		assert!(kv.insert(b"hello".into(), &[]).await.unwrap().is_none());
	});
}

#[test]
fn insert_collide() {
	run(async {
		let mut kv = mkkv().await;
		kv.insert((&[17, 4]).into(), &[]).await.unwrap().unwrap();
		kv.insert(b"RV".into(), &[]).await.unwrap().unwrap();
	});
}

#[test]
fn insert_collide3() {
	run(async {
		let mut kv = mkkv().await;
		kv.insert((&[17, 4]).into(), &[]).await.unwrap().unwrap();
		kv.insert(b"RV".into(), &[]).await.unwrap().unwrap();
		kv.insert((&[167, 114]).into(), &[]).await.unwrap().unwrap();
	});
}

#[test]
fn insert_collide_dup() {
	run(async {
		let mut kv = mkkv().await;
		kv.insert((&[17, 4]).into(), &[]).await.unwrap().unwrap();
		kv.insert(b"RV".into(), &[]).await.unwrap().unwrap();
		assert!(kv.insert(b"RV".into(), &[]).await.unwrap().is_none());
	});
}

#[test]
fn find_collide() {
	run(async {
		let mut kv = mkkv().await;
		kv.insert((&[17, 4]).into(), &[]).await.unwrap().unwrap();
		kv.insert(b"RV".into(), &[]).await.unwrap().unwrap();
		kv.find((&[17, 4]).into()).await.unwrap().unwrap();
		kv.find(b"RV".into()).await.unwrap().unwrap();
	});
}

#[test]
fn remove() {
	run(async {
		let mut kv = mkkv().await;
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
		let mut kv = mkkv().await;
		let tags = &RefCell::new([
			Some(kv.insert((&[17, 4]).into(), &[]).await.unwrap().unwrap()),
			Some(kv.insert(b"RV".into(), &[]).await.unwrap().unwrap()),
			Some(kv.insert((&[167, 114]).into(), &[]).await.unwrap().unwrap()),
			Some(kv.insert(b"hi".into(), &[]).await.unwrap().unwrap()),
		]);
		let mut it = Default::default();
		ShareNrkv::new(&mut kv)
			.next_batch(&mut it, move |tag| async move {
				for t in tags.borrow_mut().iter_mut() {
					if *t == Some(tag) {
						*t = None;
						return Ok(true);
					}
				}
				panic!("tag not in tags");
			})
			.await
			.unwrap();
		assert!(
			tags.borrow().iter().all(|t| t.is_none()),
			"not all items visited {:?}",
			tags
		);
	});
}

#[test]
fn user_data() {
	run(async {
		let mut kv = mkkv().await;
		let tag = kv.insert(b"hello".into(), &[]).await.unwrap().unwrap();
		kv.write_user_data(tag, 4, b"I'm a sheep").await.unwrap();
		let mut buf = [0; 16];
		kv.read_user_data(tag, 0, &mut buf).await.unwrap();
		assert_eq!(buf, *b"\0\0\0\0I'm a sheep\0");
	});
}

#[test]
fn next_batch_child_step_reset() {
	run(async {
		let mut kv = mkkv().await;
		kv.insert(b"\0".into(), &[]).await.unwrap().unwrap();
		assert!(kv.remove(b"\0".into()).await.unwrap());
		let b = kv.insert(b"J\x07\xC7\xC7\xC7\xC7\xC7\xF1\xF1\xF1\0\0\0\0J\x07\xC7\xC7\xC7\xC7\xC7\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\x10\0\0\0\0\0\0\0{\xF1\xF1\xF1\xF1\xD1\xF1\xF1\xF1\xF1\xF1\xFF\xFF\xFF\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x15\x0e\xFB\xF1\x0e\x0e\x0e\x0e\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xF1\xC7\xF1\xC7\xC7\xC7\xC7\xC7\xC7\xC7\xC7\xC7\0\xC7\xC7\xC7\xC7\xC7\xC7\xC7\xC7\xC7\0\0\0\0\0\0\0\0\0\xC7\0\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\0\0\0\0\0\0\0\0\0\0\0H\0\0\0\0\0]\0\0\xC7\xC7\xC7\xC7\xC7\xC7\xC7\xC7\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".into(), &[]).await.unwrap().unwrap();
		kv.insert(b"\0".into(), &[]).await.unwrap().unwrap();
		assert!(kv.remove(b"\0".into()).await.unwrap());
		let d = kv.insert(b"\0".into(), &[]).await.unwrap().unwrap();
		let mut state = Default::default();
		let (x, y) = (&Cell::new(Some(b)), &Cell::new(Some(d)));
		ShareNrkv::new(&mut kv)
			.next_batch(&mut state, |tag| async move {
				(x.get() == Some(tag)).then(|| x.take());
				(y.get() == Some(tag)).then(|| y.take());
				Ok(true)
			})
			.await
			.unwrap();
		assert!(x.get().is_none());
		assert!(y.get().is_none());
	});
}

#[test]
fn reinsert_many_find_with_next() {
	run(async {
		let mut kv = mkkv().await;
		for _ in 0..32 {
			kv.insert(b"\0".into(), &[]).await.unwrap().unwrap();
			assert!(kv.remove(b"\0".into()).await.unwrap());
		}
		let tag = kv.insert(b"\0".into(), &[]).await.unwrap().unwrap();
		let t = &Cell::new(Some(tag));
		let mut state = Default::default();
		let kv = &ShareNrkv::new(&mut kv);
		kv.next_batch(&mut state, |tag| async move {
			if kv.borrow_mut().read_key(tag, &mut []).await? != 0 {
				assert_eq!(t.take().unwrap(), tag);
			}
			Ok(true)
		})
		.await
		.unwrap();
		assert!(t.get().is_none());
	});
}
