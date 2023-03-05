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
pub struct ExtMap {
	unix: Option<NonZeroU8>,
	mtime: Option<NonZeroU8>,
	unknown: Vec<UnknownExt>,
	is_dirty: bool,
}

#[derive(Debug)]
pub struct UnknownExt {
	pub id: NonZeroU8,
	pub data_len: u8,
}

impl ExtMap {
	/// Parse extensions in filesystem header
	pub fn parse(mut data: &[u8]) -> Self {
		let mut unix @ mut mtime = None;
		let mut unknown = vec![];
		let mut id = NonZeroU8::MIN;
		while !data.is_empty() {
			let (&[nlen, dlen, flen], rem) = data.split_array_ref::<3>();
			let (name, rem) = rem.split_at(nlen.into());
			let (_, rem) = rem.split_at(flen.into());
			data = rem;
			match name {
				b"" => {}
				b"unix" => unix = Some(id),
				b"mtime" => mtime = Some(id),
				_ => unknown.push(UnknownExt { id, data_len: dlen }),
			}
			id = id.checked_add(1).unwrap();
		}
		Self { unix, mtime, unknown, is_dirty: false }
	}

	fn new_id(&mut self) -> NonZeroU8 {
		let mut id = 1;
		id += u8::from(self.unix.is_some());
		id += u8::from(self.mtime.is_some());
		id += u8::try_from(self.unknown.len()).unwrap();
		self.is_dirty = true;
		id.try_into().unwrap()
	}

	/// Map extensions to ID or insert a new one.
	pub fn get_id_or_insert(&mut self, ext: Ext) -> NonZeroU8 {
		match ext {
			Ext::Unix if let Some(id) = self.unix => id,
			Ext::MTime if let Some(id) = self.mtime => id,
			_ => {
				let id = self.new_id();
				match ext {
					Ext::Unix => *self.unix.insert(id),
					Ext::MTime => *self.mtime.insert(id),
				}
			}
		}
	}

	/// Map ID to extension.
	pub fn get_ext(&self, id: NonZeroU8) -> Result<Ext, Option<&UnknownExt>> {
		Ok(match id {
			_ if Some(id) == self.unix => Ext::Unix,
			_ if Some(id) == self.mtime => Ext::MTime,
			_ => return Err(self.unknown.iter().find(|e| e.id == id)),
		})
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
