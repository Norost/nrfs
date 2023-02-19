use crate::data::record::RecordRef;

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub(crate) struct Object {
	pub root: [RecordRef; 4],
}

raw!(Object);
