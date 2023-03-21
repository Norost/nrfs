use crate::util;

use {
	super::{
		super::{Depth, Tree},
		Dev, Object, Resource, RootIndex,
	},
	crate::Error,
};

impl<'a, D: Dev, R: Resource> Object<'a, D, R> {
	/// Zero out a range of data.
	///
	/// This is more efficient than [`Object::write`] for clearing large regions.
	pub async fn write_zeros(&self, offset: u64, len: u64) -> Result<u64, Error<D>> {
		trace!(
			"write_zeros id {:#x}, offset {}, len {}",
			self.id,
			offset,
			len,
		);

		let max_len = self.max_len();
		let len = if offset >= max_len || len == 0 {
			return Ok(0);
		} else if len > max_len - offset {
			max_len - offset
		} else {
			len
		};

		let end = offset.saturating_add(len);

		let rec_size_p2 = self.cache.max_rec_size().to_raw();

		let (offt, index) = util::divmod_p2(offset, rec_size_p2);
		let (end_offt, end_index) = util::divmod_p2(end, rec_size_p2);

		let (root, mut offt) = self
			.offset_to_tree(offt)
			.expect("offset is not addressable");
		let (end_root, end_offt) = self
			.offset_to_tree(end_offt)
			.map_or((None, 0), |(r, o)| (Some(r), o));

		let tree = |root| Tree::object(self.cache, self.id, root);

		// Check if we just need to zero out the middle of a single record.
		if Some(root) == end_root && offt == end_offt {
			tree(root)
				.get(Depth::D0, offt)
				.await?
				.write_zeros(index, end_index - index);
			return Ok(len);
		}

		// Trim leftmost record.
		if index != 0 {
			tree(root)
				.get(Depth::D0, offt)
				.await?
				.write_zeros(index, (1 << rec_size_p2) - index);
			offt += 1;
		}

		// Clear middle records.
		for r in root..=RootIndex::I3 {
			if end_root.is_some_and(|er| r >= er) {
				break;
			}
			let t = tree(r);
			t.set_zeros(offt, t.max_offset() - offt).await?;
			offt = 0;
		}
		if let Some(er) = end_root {
			tree(er).set_zeros(offt, end_offt - offt).await?;

			// Trim rightmost record.
			// If end_index is 0, it needs to remain untouched.
			if end_index != 0 {
				tree(er)
					.get(Depth::D0, end_offt)
					.await?
					.write_zeros(0, end_index);
			}
		}

		Ok(len)
	}
}
