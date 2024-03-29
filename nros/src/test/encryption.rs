use {
	super::*,
	core::num::{NonZeroU32, NonZeroU8},
};

#[test]
fn write_read_nocache() {
	let s = block_on(Nros::new(NewConfig {
		resource: StdResource::new(),
		mirrors: vec![vec![MemDev::new(1 << 12, BlockSize::K1)]],
		magic: *b"CRYP",
		key_deriver: KeyDeriver::None { key: &[0xcc; 32] },
		cipher: CipherType::XChaCha12Poly1305,
		block_size: BlockSize::K1,
		max_record_size: MaxRecordSize::K1,
		compression: Compression::None,
		cache_size: 0,
	}))
	.unwrap();

	run(&s, async {
		let obj = s.create().await.unwrap();
		obj.write(0, &[1; 1 << 13]).await.unwrap();

		let obj = s.get(0);
		let buf = &mut [2; 1 << 13];
		obj.read(0, buf).await.unwrap();
		assert_eq!(*buf, [1; 1 << 13]);

		Ok(())
	});
}

#[test]
fn mkfs_argon2id() {
	let s = block_on(Nros::new(NewConfig {
		resource: StdResource::new(),
		mirrors: vec![vec![MemDev::new(1 << 12, BlockSize::K1)]],
		magic: *b"CRYP",
		key_deriver: KeyDeriver::Argon2id {
			password: b"test",
			m: NonZeroU32::new(1024).unwrap(),
			p: NonZeroU8::MIN,
			t: NonZeroU32::MIN,
		},
		cipher: CipherType::XChaCha12Poly1305,
		block_size: BlockSize::K1,
		max_record_size: MaxRecordSize::K1,
		compression: Compression::None,
		cache_size: 0,
	}))
	.unwrap();
	let devices = block_on(s.unmount()).unwrap();
	block_on(Nros::load(LoadConfig {
		resource: StdResource::new(),
		devices,
		magic: *b"CRYP",
		cache_size: 0,
		allow_repair: false,
		retrieve_key: &mut |_| Some(KeyPassword::Password((*b"test").into())),
	}))
	.unwrap();
}
