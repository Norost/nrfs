use {
	super::Dir,
	crate::{
		ext::{MTime, Unix},
		item::ItemExt,
		Dev, Error, Ext, ExtMap,
	},
	core::{cell::RefCell, fmt, num::NonZeroU8},
};

#[repr(C)] // to avoid rearrangment that may cause unnecessary copies
pub(crate) struct DirHeader<'a> {
	ext_map: &'a RefCell<ExtMap>,
	pub blocks_used: u32,
	pub highest_block: u32,
	pub heap_id: u64,
	pub heap_length: u64,
	pub heap_allocated: u64,
	ext: [u8; 96],
}

impl<'a> DirHeader<'a> {
	/// Decode extension information in the extension blocks of an item.
	pub fn decode_ext(&self, data: &[u8]) -> ItemExt {
		let mut ext = ItemExt::default();
		for e in self.ext() {
			match e {
				DirExt::Unix { offset } => {
					let o = usize::from(offset);
					ext.unix = Some(Unix::from_raw(data[o..o + 8].try_into().unwrap()));
				}
				DirExt::MTime { offset } => {
					let o = usize::from(offset);
					ext.mtime = Some(MTime::from_raw(data[o..o + 8].try_into().unwrap()));
				}
				DirExt::Unknown { .. } => todo!(),
			}
		}
		ext
	}

	/// Determine the amount of item slots necessary to store all extension data.
	pub fn ext_slots(&self) -> u16 {
		let end = self
			.ext()
			.map(|ext| match ext {
				DirExt::Unix { offset } => offset + 8,
				DirExt::MTime { offset } => offset + 8,
				DirExt::Unknown { .. } => todo!(),
			})
			.max()
			.unwrap_or(0);
		(end + 15) / 16
	}

	/// Iterate over extensions.
	pub fn ext(&self) -> impl Iterator<Item = DirExt> + '_ {
		let mut i = 0;
		core::iter::from_fn(move || {
			while let Some(&id) = self.ext.get(i) {
				i += 1;
				let Some(id) = NonZeroU8::new(id) else { continue };
				let f16 = |i: &mut _| {
					*i += 2;
					u16::from_le_bytes(self.ext[*i - 2..*i].try_into().unwrap())
				};
				return Some(match self.ext_map.borrow_mut().get_ext(id) {
					Ok(Ext::Unix) => DirExt::Unix { offset: f16(&mut i) },
					Ok(Ext::MTime) => DirExt::MTime { offset: f16(&mut i) },
					Err(Some(e)) => {
						i += usize::from(e.data_len);
						DirExt::Unknown
					}
					Err(None) => todo!(),
				});
			}
			None
		})
	}
}

impl fmt::Debug for DirHeader<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		struct Ext<'x>(&'x DirHeader<'x>);

		impl fmt::Debug for Ext<'_> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				let mut f = f.debug_list();
				for e in self.0.ext() {
					f.entry(&e);
				}
				f.finish()
			}
		}

		let mut f = f.debug_struct(stringify!(DirHeader));
		f.field("blocks_used", &self.blocks_used);
		f.field("highest_block", &self.highest_block);
		f.field("heap_id", &self.heap_id);
		f.field("heap_length", &self.heap_length);
		f.field("heap_allocated", &self.heap_allocated);
		f.field("ext", &Ext(self));
		f.finish()
	}
}

#[derive(Debug)]
pub(crate) enum DirExt {
	Unix { offset: u16 },
	MTime { offset: u16 },
	Unknown,
}

macro_rules! into {
	($f:ident $v:ident $n:ident $r:ty) => {
		pub fn $f(self) -> Option<$r> {
			let Self::$v { $n } = self else { return None };
			Some($n)
		}
	};
}

impl DirExt {
	into!(into_unix Unix offset u16);
	into!(into_mtime MTime offset u16);
}

impl<'a, D: Dev> Dir<'a, D> {
	/// Get directory header.
	pub(crate) async fn header(&self) -> Result<DirHeader<'a>, Error<D>> {
		trace!("header {:#x}", self.key.id);
		let mut buf = [0; 128];
		self.fs.get(self.key.id).read(0, &mut buf).await?;

		let [a, b, c, d, e, f, g, h, buf @ ..] = buf;
		let blocks_used = u32::from_le_bytes([a, b, c, d]);
		let highest_block = u32::from_le_bytes([e, f, g, h]);
		let [a, b, c, d, e, f, g, h, buf @ ..] = buf;
		let heap_id = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
		let [a, b, c, d, e, f, g, h, buf @ ..] = buf;
		let heap_length = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
		let [a, b, c, d, e, f, g, h, buf @ ..] = buf;
		let heap_allocated = u64::from_le_bytes([a, b, c, d, e, f, g, h]);

		Ok(DirHeader {
			ext_map: &self.fs.ext,
			blocks_used,
			highest_block,
			heap_id,
			heap_length,
			heap_allocated,
			ext: buf,
		})
	}

	/// Save directory header.
	pub(crate) async fn set_header(&self, hdr: DirHeader<'_>) -> Result<(), Error<D>> {
		trace!("set_header {:#x} {:?}", self.key.id, hdr);
		let mut buf = [0; 128];
		buf[0..4].copy_from_slice(&hdr.blocks_used.to_le_bytes());
		buf[4..8].copy_from_slice(&hdr.highest_block.to_le_bytes());
		buf[8..16].copy_from_slice(&hdr.heap_id.to_le_bytes());
		buf[16..24].copy_from_slice(&hdr.heap_length.to_le_bytes());
		buf[24..32].copy_from_slice(&hdr.heap_allocated.to_le_bytes());
		buf[32..128].copy_from_slice(&hdr.ext);
		self.fs.get(self.key.id).write(0, &buf).await?;
		Ok(())
	}
}
