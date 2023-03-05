use {
	super::DirHeader,
	crate::{Dev, Dir, Error, Name},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Find an entry by name.
	///
	/// Returns index of item data & item data itself.
	/// If not found, returns index of first slot that can contain a new element.
	pub(in super::super) async fn find(
		&self,
		hdr: &DirHeader<'_>,
		name: &Name,
	) -> Result<FindResult, Error<D>> {
		trace!("find {:?}", name);
		let data_blocks = 1 + hdr.ext_slots();
		let need_blocks = super::name_blocks(name) + data_blocks;

		let mut index = 0;
		let mut unused_index = None;
		let mut unused_blocks = 0;
		let mut cur_unused_index = 0;

		while index < hdr.highest_block {
			let cur_name;
			(cur_name, index) = self.item_name(index).await?;
			let Some(cur_name) = cur_name else {
				if unused_index.is_none() {
					unused_blocks += 1;
					if unused_blocks >= need_blocks {
						unused_index = Some(cur_unused_index);
					}
				}
				continue
			};
			unused_blocks = 0;
			if name == &*cur_name {
				return Ok(FindResult::Found { data_index: index });
			}
			index += u32::from(data_blocks);
			cur_unused_index = index;
		}
		Ok(FindResult::NotFound { data_index: unused_index.unwrap_or(cur_unused_index) })
	}
}

pub(crate) enum FindResult {
	Found { data_index: u32 },
	NotFound { data_index: u32 },
}
