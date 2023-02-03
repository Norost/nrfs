mod chain;
mod mirror;

use {
	super::block_on,
	crate::{
		dev::{Allocator, Buf},
		*,
	},
};

async fn new(mirrors: Vec<Vec<dev::MemDev>>) -> Nros<dev::MemDev, StdResource> {
	Nros::new(NewConfig {
		magic: *b"TEST",
		resource: StdResource::new(),
		mirrors,
		block_size: BlockSize::K1,
		max_record_size: MaxRecordSize::K16,
		compression: Compression::None,
		cipher: CipherType::NoneXxh3,
		key_deriver: KeyDeriver::None { key: &[0; 32] },
		cache_size: 1 << 14,
	})
	.await
	.unwrap()
}

async fn load(devices: Vec<dev::MemDev>) -> Nros<dev::MemDev, StdResource> {
	Nros::load(LoadConfig {
		magic: *b"TEST",
		resource: StdResource::new(),
		devices,
		cache_size: 1 << 14,
		retrieve_key: &mut |_| unreachable!(),
		allow_repair: true,
	})
	.await
	.unwrap()
}
