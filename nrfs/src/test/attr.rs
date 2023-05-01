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
