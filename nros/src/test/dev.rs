use {
	super::{block_on, Set256},
	crate::{
		storage::dev::{Allocator, Buf, Dev, DevSet, MemDev},
		BlockSize, Compression, MaxRecordSize, Resource, StdResource,
	},
};

/// Write to & read from a [`MemDev`].
#[test]
fn memdev_read_write() {
	block_on(async {
		let dev = MemDev::new(16, BlockSize::B512);

		let mut buf1k = dev.allocator().alloc(2 * 512).await.unwrap();
		assert_eq!(buf1k.get().len(), 2 * 512);

		let mut buf2k = dev.allocator().alloc(4 * 512).await.unwrap();
		assert_eq!(buf2k.get().len(), 4 * 512);

		buf1k.get_mut().fill(0x11);
		dev.write(0, buf1k).await.unwrap();
		let rd1k = dev.read(0, 2 * 512).await.unwrap();
		assert_eq!(rd1k.get(), [0x11; 2 * 512]);

		buf2k.get_mut().fill(0x22);
		dev.write(1, buf2k.clone()).await.unwrap();

		let rd1k = dev.read(0, 3 * 512).await.unwrap();
		assert_eq!(&rd1k.get()[..512], [0x11; 512]);
		assert_eq!(&rd1k.get()[512..], [0x22; 512 * 2]);
	})
}

/// Create [`DevSet`] with one device.
#[test]
fn devset_1_create() {
	block_on(async {
		let dev = MemDev::new(16, BlockSize::B512);
		let _ = DevSet::new(
			StdResource::new(),
			[[dev]],
			BlockSize::B512,
			MaxRecordSize::B512,
			Compression::None,
		)
		.await
		.unwrap();
	})
}

/// Write to & read from a [`DevSet`] with one device.
#[test]
fn devset_1_read_write() {
	block_on(async {
		let dev = MemDev::new(16, BlockSize::B512);
		let set = DevSet::new(
			StdResource::new(),
			[[dev]],
			BlockSize::B512,
			MaxRecordSize::B512,
			Compression::None,
		)
		.await
		.unwrap();

		let mut buf1k = set.alloc(2 * 512).await.unwrap();
		assert_eq!(buf1k.get().len(), 2 * 512);

		let mut buf2k = set.alloc(4 * 512).await.unwrap();
		assert_eq!(buf2k.get().len(), 4 * 512);

		buf1k.get_mut().fill(0x11);
		set.write(1.try_into().unwrap(), buf1k, Set256::set_all())
			.await
			.unwrap();
		let (rd1k, _) = set
			.read(1.try_into().unwrap(), 2 * 512, &Default::default())
			.await
			.unwrap()
			.unwrap();
		assert_eq!(rd1k.get(), [0x11; 2 * 512]);

		buf2k.get_mut().fill(0x22);
		set.write(2.try_into().unwrap(), buf2k, Set256::set_all())
			.await
			.unwrap();

		let (rd1k, _) = set
			.read(1.try_into().unwrap(), 5 * 512, &Default::default())
			.await
			.unwrap()
			.unwrap();
		assert_eq!(&rd1k.get()[..512], [0x11; 512]);
		assert_eq!(&rd1k.get()[512..], [0x22; 512 * 4]);
	})
}
