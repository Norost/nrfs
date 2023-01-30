use super::*;

#[test]
fn write_read_nocache() {
	let s = block_on(Nros::new(NewConfig {
		resource: StdResource::new(),
		mirrors: vec![vec![MemDev::new(1 << 12, BlockSize::K1)]],
		magic: *b"CRYP",
		key_deriver: KeyDeriver::None { key: &[0xcc; 32] },
		cipher: CipherType::ChaCha8Poly1305,
		block_size: BlockSize::K1,
		max_record_size: MaxRecordSize::K1,
		compression: Compression::None,
		cache_size: 0,
	}))
	.unwrap();

	let bg = Default::default();
	run2(&bg, async {
		let obj = s.create(&bg).await.unwrap();
		obj.resize(1 << 13).await.unwrap();
		obj.write(0, &[0xcc; 1 << 13]).await.unwrap();

		let obj = s.get(&bg, 0).await.unwrap();
		let buf = &mut [0; 1 << 13];
		obj.read(0, buf).await.unwrap();
		assert_eq!(*buf, [0xcc; 1 << 13]);
	});
	block_on(bg.drop()).unwrap();
}
