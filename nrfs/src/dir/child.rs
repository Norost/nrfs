use crate::Idx;

/// A file or directory that is a child of another directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Child {
	File(Idx),
	Dir(u64),
}
