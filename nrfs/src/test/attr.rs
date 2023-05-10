use super::*;

#[test]
fn add_attr() {
	let fs = new();
	run(&fs, async {
		let f = mkfile(&fs.root_dir(), b"file").await;
		f.set_attr(b"hello".into(), b"world")
			.await
			.unwrap()
			.unwrap();
	});
}

#[test]
fn get_attr() {
	let fs = new();
	run(&fs, async {
		let f = mkfile(&fs.root_dir(), b"file").await;
		f.set_attr(b"hello".into(), b"world")
			.await
			.unwrap()
			.unwrap();
		let v = f.attr(b"hello".into()).await.unwrap().unwrap();
		assert_eq!(b"world", &*v);
	});
}

#[test]
fn mod_attr() {
	let fs = new();
	run(&fs, async {
		let f = mkfile(&fs.root_dir(), b"file").await;
		f.set_attr(b"hello".into(), b"world")
			.await
			.unwrap()
			.unwrap();
		f.set_attr(b"hello".into(), b"earth")
			.await
			.unwrap()
			.unwrap();
		let v = f.attr(b"hello".into()).await.unwrap().unwrap();
		assert_eq!(b"earth", &*v);
	});
}

#[test]
fn del_attr() {
	let fs = new();
	run(&fs, async {
		let f = mkfile(&fs.root_dir(), b"file").await;
		f.set_attr(b"hello".into(), b"world")
			.await
			.unwrap()
			.unwrap();
		let r = f.del_attr(b"hello".into()).await.unwrap();
		assert!(r);
		assert!(f.attr(b"hello".into()).await.unwrap().is_none());
		let r = f.del_attr(b"hello".into()).await.unwrap();
		assert!(!r);
	});
}

#[test]
fn get_attr_multi() {
	let fs = new();
	run(&fs, async {
		let f = mkfile(&fs.root_dir(), b"file").await;
		f.set_attr(b"hello".into(), b"world")
			.await
			.unwrap()
			.unwrap();
		f.set_attr(b"cheers".into(), b"mate")
			.await
			.unwrap()
			.unwrap();
		let v = f.attr(b"hello".into()).await.unwrap().unwrap();
		assert_eq!(b"world", &*v);
		let v = f.attr(b"cheers".into()).await.unwrap().unwrap();
		assert_eq!(b"mate", &*v);
	});
}

#[test]
fn list_attr_keys() {
	let fs = new();
	run(&fs, async {
		let f = mkfile(&fs.root_dir(), b"file").await;
		f.set_attr(b"hello".into(), b"world")
			.await
			.unwrap()
			.unwrap();
		f.set_attr(b"cheers".into(), b"mate")
			.await
			.unwrap()
			.unwrap();
		f.set_attr(b"hello".into(), b"globe")
			.await
			.unwrap()
			.unwrap();
		let keys = f.attr_keys().await.unwrap();
		assert_eq!(keys.len(), 2);
		assert!(keys.iter().map(|k| &***k).find(|k| k == b"hello").is_some());
		assert!(keys
			.iter()
			.map(|k| &***k)
			.find(|k| k == b"cheers")
			.is_some());
		for k in keys {
			assert!(matches!(&**k, b"hello" | b"cheers"));
		}
	});
}

#[test]
fn list_attr_empty() {
	let fs = new();
	run(&fs, async {
		let f = mkfile(&fs.root_dir(), b"file").await;
		let keys = f.attr_keys().await.unwrap();
		assert!(keys.is_empty());
	});
}

#[test]
fn many_attrs() {
	let fs = new_cap(8, BlockSize::B512, MaxRecordSize::B512, 1 << 20);
	run(&fs, async {
		for i in 0..512u16 {
			let f = mkfile(&fs.root_dir(), &i.to_le_bytes()).await;
			f.set_attr((&i.to_le_bytes()).into(), b"test")
				.await
				.unwrap()
				.unwrap();
		}
		for i in 0..512u16 {
			let k = &i.to_le_bytes();
			let item = fs.root_dir().search(k.into()).await.unwrap();
			let item = fs.item(item.unwrap().key);
			let keys = item.attr_keys().await.unwrap();
			assert_eq!(keys.len(), 1);
			assert_eq!(&**keys[0], k);
			let val = item.attr(k.into()).await.unwrap();
			assert_eq!(&*val.unwrap(), b"test");
		}
	});
}
