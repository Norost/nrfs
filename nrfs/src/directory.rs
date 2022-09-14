use endian::u64le;

#[repr(C, align(16))]
pub struct Header {
	free_block_offset: [u8; 6],
	_reserved: [u8; 10],
}

raw!(Header);

#[repr(C, align(16))]
pub struct Entry {
	key_offset_or_next_table_length: [u8; 6],
	key_length: u8,
	flags: u8,
	object_index_or_next_table_offset: u64le,
}

raw!(Entry);
