use {
	crate::storage::{Storage, Write},
	endian::u64le,
	rangemap::RangeSet,
};

#[repr(C, align(16))]
struct Entry {
	lba: u64le,
	size: u64le,
}

raw!(Entry);

#[derive(Default)]
pub struct Allocator {
	/// Map of *allocated* blocks.
	alloc_map: RangeSet<u64>,
}

impl Allocator {
	pub fn alloc<S>(&mut self, blocks: u64, sto: &S) -> Option<u64>
	where
		S: Storage,
	{
		for r in self.alloc_map.gaps(&(1..sto.block_count())) {
			if r.end - r.start >= blocks {
				self.alloc_map.insert(r.start..r.start + blocks);
				return Some(r.start);
			}
		}
		None
	}

	pub fn serialize_full<S>(&mut self, sto: &mut S) -> Result<(), SerializeError<S>>
	where
		S: Storage,
	{
		let (len_min, len_max) = self.alloc_map.iter().size_hint();
		assert_eq!(Some(len_min), len_max);
		let lba = self
			.alloc(len_min.try_into().unwrap(), sto)
			.ok_or(SerializeError::NotEnoughSpace)?;
		let mut wr = sto.write(lba, len_min).map_err(SerializeError::Storage)?;
		for (w, r) in wr.get_mut().chunks_exact_mut(16).zip(self.alloc_map.iter()) {
			let len = r.end - r.start;
			w[..8].copy_from_slice(&r.start.to_le_bytes());
			w[8..].copy_from_slice(&len.to_le_bytes());
		}
		Ok(())
	}
}

pub enum SerializeError<S: Storage> {
	NotEnoughSpace,
	Storage(S::Error),
}
