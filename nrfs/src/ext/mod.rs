mod mtime;
mod unix;

pub use {mtime::MTime, unix::Unix};

use core::num::NonZeroU8;

/// Extensions
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Ext {
	Unix,
	MTime,
}

/// Extension to ID map.
#[derive(Debug, Default)]
pub(crate) struct ExtMap {
	unix: Option<(NonZeroU8, usize)>,
	mtime: Option<(NonZeroU8, usize)>,
	unknown: Vec<UnknownExt>,
	end: usize,
}

#[derive(Debug)]
pub(crate) struct UnknownExt {
	pub id: NonZeroU8,
	pub data_len: u8,
}

impl ExtMap {
	/// Parse extensions in filesystem header
	pub fn parse(mut data: &[u8]) -> Self {
		let og_len = data.len();
		let mut unix @ mut mtime = None;
		let mut unknown = vec![];
		while data.len() >= 4 {
			let (&[id, nlen, dlen, flen], rem) = data.split_array_ref::<4>();
			let Some(id) = NonZeroU8::new(id) else { break };
			let (name, rem) = rem.split_at(nlen.into());
			let offt = og_len - rem.len();
			let (_, rem) = rem.split_at(flen.into());
			data = rem;
			match name {
				b"unix" => unix = Some((id, offt)),
				b"mtime" => mtime = Some((id, offt)),
				_ => unknown.push(UnknownExt { id, data_len: dlen }),
			}
		}
		let end = og_len - data.len();
		Self { unix, mtime, unknown, end }
	}

	fn new_id(&mut self) -> NonZeroU8 {
		for id in 1..255 {
			let id = NonZeroU8::new(id).unwrap();
			if !self.unix.is_some_and(|(i, _)| i == id)
				&& !self.mtime.is_some_and(|(i, _)| i == id)
				&& self.unknown.iter().all(|e| e.id != id)
			{
				return id;
			}
		}
		panic!("no free extension IDs");
	}

	/// Map extensions to ID or insert a new one.
	pub fn get_id(&self, ext: Ext) -> Option<(NonZeroU8, usize)> {
		match ext {
			Ext::Unix => self.unix,
			Ext::MTime => self.mtime,
		}
	}

	/// Map ID to extension.
	pub fn get_ext(&self, id: NonZeroU8) -> Result<Ext, Option<&UnknownExt>> {
		Ok(match id {
			_ if self.unix.is_some_and(|(i, _)| i == id) => Ext::Unix,
			_ if self.mtime.is_some_and(|(i, _)| i == id) => Ext::MTime,
			_ => return Err(self.unknown.iter().find(|e| e.id == id)),
		})
	}
}

impl<D: nros::Dev> super::Nrfs<D> {
	/// Map extension to ID or insert a new one.
	pub(crate) fn get_id_or_insert(&self, ext: Ext) -> (NonZeroU8, usize) {
		trace!("get_id_or_insert {:?}", ext);
		let map = &mut *self.ext.borrow_mut();
		if let Some(id) = map.get_id(ext) {
			return id;
		}
		let data = &mut self.storage.header_data()[16..];
		let id = map.new_id();
		let offt;
		match ext {
			Ext::Unix => {
				let d = &mut data[map.end..map.end + 4 + 4 + 8];
				d[..4].copy_from_slice(&[id.into(), 4, 8, 8]);
				d[4..8].copy_from_slice(b"unix");
				d[8..].copy_from_slice(&[0; 8]);
				offt = map.end + 8;
				map.unix = Some((id, offt));
				map.end += d.len();
			}
			Ext::MTime => {
				let d = &mut data[map.end..map.end + 4 + 5 + 8];
				d[..4].copy_from_slice(&[id.into(), 5, 8, 8]);
				d[4..9].copy_from_slice(b"mtime");
				d[9..].copy_from_slice(&[0; 8]);
				offt = map.end + 9;
				map.mtime = Some((id, offt));
				map.end += d.len();
			}
		}
		trace!("--> new {:?} {:?}", id, offt);
		(id, offt)
	}
}

#[derive(Clone, Copy, Default, Debug)]
#[cfg_attr(any(test, fuzzing), derive(arbitrary::Arbitrary))]
pub struct EnableExt(u8);

macro_rules! ext {
	($a:ident $g:ident $b:literal) => {
		pub fn $a(&mut self) -> &mut Self {
			self.0 |= 1 << $b;
			self
		}

		pub fn $g(&self) -> bool {
			self.0 & 1 << $b != 0
		}
	};
}

impl EnableExt {
	ext!(add_unix unix 0);
	ext!(add_mtime mtime 1);
}
