use {
	super::{
		super::{header::DirExt, DirNewItem},
		Dir,
	},
	crate::{
		dir::header::DirHeader,
		ext::{MTime, Unix},
		item::{ItemData, ItemExt},
		Dev, DirKey, Error, Name,
	},
};

const NAME_BLK: u8 = 0b0010;
const NAME_FIRST_BLK: u8 = 0b0110;
const NAME_LAST_BLK: u8 = 0b1010;

impl<'a, D: Dev> Dir<'a, D> {
	/// Get raw item data.
	async fn item_get(&self, index: u32, offset: u16, buf: &mut [u8]) -> Result<(), Error<D>> {
		trace!("item_get {:?}+{} {:?}", index, offset, buf.len());
		let offt = super::index_to_offset(index) + u64::from(offset);
		self.fs.get(self.key.id).read(offt, buf).await?;
		Ok(())
	}

	/// Set arbitrary item data.
	async fn item_set(&self, index: u32, offset: u16, data: &[u8]) -> Result<(), Error<D>> {
		trace!("item_set {:?}+{} {:x?}", index, offset, data);
		let offt = super::index_to_offset(index) + u64::from(offset);
		self.fs.get(self.key.id).write(offt, data).await?;
		Ok(())
	}

	/// Insert a new item at the given index.
	///
	/// The location *must* have been found with [`Self::find`],
	/// which in turn must have returned [`NotFound`].
	///
	/// # Returns
	///
	/// The index of the data block.
	pub(crate) async fn item_insert(
		&self,
		hdr: &mut DirHeader<'_>,
		name_index: u32,
		item: DirNewItem<'_>,
	) -> Result<u32, Error<D>> {
		trace!("item_insert {:#x} {:?} {:?}", self.key.id, name_index, item);
		let name_blocks = super::name_blocks(item.name);
		let ext_slots = hdr.ext_slots();
		let blocks = name_blocks + 1 + ext_slots;

		let mut buf = vec![0; usize::from(blocks) * 16];
		let (name, rem) = buf.split_at_mut(usize::from(name_blocks) * 16);
		let (data, ext) = rem.split_at_mut(16);

		let mut it_r = item.name.chunks(15);
		let mut it_w = name.chunks_mut(16);
		for (r, w) in (&mut it_r).zip(&mut it_w) {
			w[1..1 + r.len()].copy_from_slice(r);
			w[0] = (r.len() as u8) << 4 | NAME_BLK;
		}
		name[0] |= NAME_FIRST_BLK;
		name.chunks_mut(16).last().expect("non-empty name")[0] |= NAME_LAST_BLK;

		data.copy_from_slice(&data_to_raw(&item.data));
		for e in hdr.ext() {
			match e {
				DirExt::Unix { offset } => {
					if let Some(unix) = item.ext.unix {
						let o = usize::from(offset);
						ext[o..o + 8].copy_from_slice(&unix.into_raw());
					}
				}
				DirExt::MTime { offset } => {
					if let Some(mtime) = item.ext.mtime {
						let o = usize::from(offset);
						ext[o..o + 8].copy_from_slice(&mtime.into_raw());
					}
				}
				DirExt::Unknown { .. } => todo!(),
			}
		}
		for w in ext.chunks_mut(16) {
			w[0] |= 1;
		}

		self.item_set(name_index, 0, &buf).await?;

		hdr.blocks_used += u32::from(blocks);
		hdr.highest_block = hdr.highest_block.max(name_index + u32::from(blocks));
		Ok(name_index + u32::from(name_blocks))
	}

	/// Erase an item.
	///
	/// This does not destroy the item itself.
	pub(crate) async fn item_erase(
		&self,
		hdr: &mut DirHeader<'_>,
		data_index: u32,
	) -> Result<(), Error<D>> {
		let mut name_index = data_index;
		let mut buf = [0; 16];
		let obj = self.fs.get(self.key.id);

		while name_index > 0 {
			name_index -= 1;
			obj.read(super::index_to_offset(name_index), &mut buf)
				.await?;
			if buf[0] & 3 != 0b10 {
				name_index += 1;
				break;
			}
		}

		let blocks = data_index - name_index + 1 + u32::from(hdr.ext_slots());
		obj.write_zeros(super::index_to_offset(name_index), u64::from(blocks) * 16)
			.await?;

		hdr.blocks_used -= blocks;
		if hdr.highest_block == name_index + blocks {
			hdr.highest_block = name_index;
		}

		Ok(())
	}

	/// Destroy an item.
	pub(crate) async fn item_destroy(
		&self,
		hdr: &mut DirHeader<'_>,
		data_index: u32,
	) -> Result<(), Error<D>> {
		trace!("item_destroy {}", data_index);

		let mut buf = [0; 16];
		let obj = self.fs.get(self.key.id);

		obj.read(super::index_to_offset(data_index), &mut buf)
			.await?;
		let data = raw_to_data(&buf);

		self.item_erase(hdr, data_index).await?;

		match data.ty {
			ItemData::TY_NONE => todo!(),
			ItemData::TY_DIR => {
				let dir = DirKey { dir: u64::MAX, index: u32::MAX, id: data.id_or_offset };
				let dir = Dir::new(self.fs, dir);
				let h = dir.header().await?;
				self.fs.get(h.heap_id).dealloc().await?;
				self.fs.get(data.id_or_offset).dealloc().await?;
			}
			ItemData::TY_SYM | ItemData::TY_FILE => {
				self.fs.get(data.id_or_offset).dealloc().await?;
			}
			ItemData::TY_EMBED_SYM | ItemData::TY_EMBED_FILE => {
				self.heap(hdr)
					.dealloc(hdr, data.id_or_offset, data.len)
					.await?;
			}
			_ => todo!(),
		}

		Ok(())
	}

	/// Get item name.
	///
	/// # Returns
	///
	/// Name and index.
	pub(crate) async fn item_name(
		&self,
		mut index: u32,
	) -> Result<(Option<Box<Name>>, u32), Error<D>> {
		trace!("item_name {}", index);

		let mut buf = [0; 16];
		let obj = self.fs.get(self.key.id);

		obj.read(super::index_to_offset(index), &mut buf).await?;
		index += 1;
		if buf[0] & 0b0111 == NAME_FIRST_BLK {
			let mut name = vec![];

			let name_len = buf[0] >> 4;
			name.extend_from_slice(&buf[1..1 + usize::from(name_len)]);
			while buf[0] & 0b1011 != NAME_LAST_BLK {
				obj.read(super::index_to_offset(index), &mut buf).await?;
				index += 1;
				let name_len = buf[0] >> 4;
				name.extend_from_slice(&buf[1..1 + usize::from(name_len)]);
			}

			Ok((Some(name.into_boxed_slice().try_into().unwrap()), index))
		} else {
			Ok((None, index))
		}
	}

	/// Get item data.
	pub(crate) async fn item_get_data(&self, data_index: u32) -> Result<ItemData, Error<D>> {
		trace!("item_get_data {:#x} {}", self.key.id, data_index);
		let mut buf = [0; 16];
		self.item_get(data_index, 0, &mut buf).await?;
		Ok(raw_to_data(&buf))
	}

	/// Get item data & extensions.
	pub(crate) async fn item_get_data_ext(
		&self,
		hdr: &DirHeader<'_>,
		data_index: u32,
	) -> Result<(ItemData, ItemExt), Error<D>> {
		trace!("item_get_data_ext {:#x} {}", self.key.id, data_index);
		let blocks = 1 + hdr.ext_slots();
		let mut buf = vec![0; usize::from(blocks) * 16];
		self.item_get(data_index, 0, &mut buf).await?;
		let (data, ext) = buf.split_array_ref::<16>();
		Ok((raw_to_data(data), hdr.decode_ext(ext)))
	}

	/// Set item data.
	pub(crate) async fn item_set_data(
		&self,
		data_index: u32,
		data: ItemData,
	) -> Result<(), Error<D>> {
		trace!("item_set_data {} {:?}", data_index, &data);
		self.item_set(data_index, 0, &data_to_raw(&data)).await
	}

	/// Set `unix` extension data.
	pub(crate) async fn item_set_unix(
		&self,
		data_index: u32,
		unix: Unix,
	) -> Result<bool, Error<D>> {
		trace!("ext_set_unix {:?} {:?}", data_index, unix);
		let hdr = self.header().await?;
		let Some(offt) = hdr.ext().find_map(|d| d.into_unix()) else { return Ok(false) };
		self.item_set(data_index + 1, offt, &unix.into_raw())
			.await?;
		Ok(true)
	}

	/// Set `mtime` extension data.
	pub(crate) async fn item_set_mtime(
		&self,
		data_index: u32,
		mtime: MTime,
	) -> Result<bool, Error<D>> {
		trace!("ext_set_mtime {:?} {:?}", data_index, mtime);
		let hdr = self.header().await?;
		let Some(offt) = hdr.ext().find_map(|d| d.into_mtime()) else { return Ok(false) };
		self.item_set(data_index + 1, offt, &mtime.into_raw())
			.await?;
		Ok(true)
	}
}

pub(crate) fn raw_to_data(raw: &[u8; 16]) -> ItemData {
	let &[ty, a, b, c, d, e, f, g, len @ ..] = raw;
	ItemData {
		ty: ty >> 1,
		id_or_offset: u64::from_le_bytes([a, b, c, d, e, f, g, 0]),
		len: u64::from_le_bytes(len),
	}
}

pub(crate) fn data_to_raw(data: &ItemData) -> [u8; 16] {
	let mut buf = [0; 16];
	buf[0] = (data.ty << 1) | 1;
	buf[1..8].copy_from_slice(&data.id_or_offset.to_le_bytes()[..7]);
	buf[8..].copy_from_slice(&data.len.to_le_bytes());
	buf
}
