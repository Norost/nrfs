use core::fmt;

pub trait Conf {
	fn header_offset(&self) -> u64;
	fn item_offset(&self) -> u16;
}

#[derive(Debug)]
pub struct DynConf {
	pub header_offset: u64,
	pub item_offset: u16,
}

impl Conf for DynConf {
	fn header_offset(&self) -> u64 {
		self.header_offset
	}
	fn item_offset(&self) -> u16 {
		self.item_offset
	}
}

pub struct StaticConf<const HEADER_OFFSET: u64, const ITEM_OFFSET: u16>;

impl<const HEADER_OFFSET: u64, const ITEM_OFFSET: u16> StaticConf<HEADER_OFFSET, ITEM_OFFSET> {
	pub const CONF: Self = Self;
}

impl<const HEADER_OFFSET: u64, const ITEM_OFFSET: u16> Conf
	for StaticConf<HEADER_OFFSET, ITEM_OFFSET>
{
	fn header_offset(&self) -> u64 {
		HEADER_OFFSET
	}
	fn item_offset(&self) -> u16 {
		ITEM_OFFSET
	}
}

impl<const HEADER_OFFSET: u64, const ITEM_OFFSET: u16> fmt::Debug
	for StaticConf<HEADER_OFFSET, ITEM_OFFSET>
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		format_args!("StaticConf<{}, {}>", HEADER_OFFSET, ITEM_OFFSET).fmt(f)
	}
}
