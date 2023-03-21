//#![cfg_attr(not(test), no_std)]
#![forbid(unused_must_use)]
#![forbid(elided_lifetimes_in_paths)]
#![feature(is_some_and)]
#![feature(iterator_try_collect)]
#![feature(nonzero_min_max)]
#![feature(cell_update)]
#![feature(split_array)]
#![feature(error_in_core)]
#![feature(if_let_guard)]

/// Tracing in debug mode only.
macro_rules! trace {
	($($arg:tt)*) => {
		if cfg!(feature = "trace") {
			$crate::trace::print_debug(&format_args!($($arg)*));
		}
		#[cfg(feature = "trace")]
		let _t = $crate::trace::Trace::new();
	};
}

#[cfg(not(feature = "trace"))]
mod trace {
	use core::fmt::Arguments;

	pub fn print_debug(_: &Arguments<'_>) {}
}

#[cfg(feature = "trace")]
mod trace {
	use core::{cell::Cell, fmt::Arguments};

	thread_local! {
		static DEPTH: Cell<usize> = Cell::new(0);
	}

	pub fn print_debug(args: &Arguments<'_>) {
		DEPTH.with(|depth| {
			eprintln!("[nrfs]{:>pad$} {}", "", args, pad = depth.get() * 2);
		})
	}

	pub struct Trace;

	impl Trace {
		pub fn new() -> Self {
			DEPTH.with(|depth| depth.update(|x| x + 1));
			Self
		}
	}

	impl Drop for Trace {
		fn drop(&mut self) {
			DEPTH.with(|depth| depth.update(|x| x - 1));
		}
	}
}

extern crate alloc;

mod config;
pub mod dir;
mod ext;
mod file;
mod item;
mod name;
#[cfg(any(test, fuzzing))]
pub mod test;

pub use {
	config::{LoadConfig, NewConfig},
	core::cell::RefCell,
	dir::{CreateError, Dir, DirDestroyError, DirKey, TransferError},
	ext::{EnableExt, MTime, Unix},
	file::{File, FileKey, LengthTooLong},
	item::{Item, ItemExt, ItemInfo, ItemKey},
	name::Name,
	nros::{
		dev, BlockSize, CipherType, Compression, Dev, KeyDeriver, KeyPassword, MaxRecordSize,
		Resource,
	},
};

use {
	core::{fmt, future::Future},
	ext::{Ext, ExtMap},
};

/// NRFS filesystem manager.
#[derive(Debug)]
pub struct Nrfs<D: Dev> {
	/// Object storage.
	storage: nros::Nros<D, nros::StdResource>,
	/// Whether this filesystem is mounted as read-only.
	read_only: bool,
	/// Extension map.
	ext: RefCell<ExtMap>,
}

impl<D: Dev> Nrfs<D> {
	const MAGIC: [u8; 4] = *b"NRFS";

	pub async fn new(config: NewConfig<'_, D>) -> Result<Self, Error<D>> {
		let NewConfig {
			mirrors,
			key_deriver,
			cipher,
			block_size,
			max_record_size,
			compression,
			cache_size,
			dir,
		} = config;
		let conf = nros::NewConfig {
			mirrors,
			key_deriver,
			cipher,
			block_size,
			max_record_size,
			compression,
			cache_size,
			resource: nros::StdResource::new(),
			magic: Self::MAGIC,
		};
		let storage = nros::Nros::new(conf).await?;
		let ext = ExtMap::default().into();
		let s = Self { storage, ext, read_only: false };
		Dir::init(&s, dir).await?;
		Ok(s)
	}

	pub async fn load(config: LoadConfig<'_, D>) -> Result<Self, Error<D>> {
		trace!("load");
		let LoadConfig { devices, cache_size, allow_repair, retrieve_key } = config;
		let conf = nros::LoadConfig {
			devices,
			cache_size,
			allow_repair,
			retrieve_key,
			resource: nros::StdResource::new(),
			magic: Self::MAGIC,
		};
		let storage = nros::Nros::load(conf).await?;
		let ext = RefCell::new(ExtMap::parse(&storage.header_data()[16..]));
		trace!("--> {:#?}", ext.borrow());
		Ok(Self { storage, ext, read_only: !allow_repair })
	}

	/// Get a reference to the root directory.
	pub fn root_dir(&self) -> Dir<'_, D> {
		let data = self.storage.header_data();
		let id = u64::from_le_bytes(data[..8].try_into().unwrap()) >> 8;
		let key = DirKey { dir: u64::MAX, index: u32::MAX, id };
		Dir::new(self, key)
	}

	pub async fn run<V, E, F>(&self, f: F) -> Result<V, E>
	where
		F: Future<Output = Result<V, E>>,
		E: From<Error<D>> + From<nros::Error<D>>,
	{
		self.storage.run(f).await
	}

	pub async fn finish_transaction(&self) -> Result<(), Error<D>> {
		self.storage.finish_transaction().await.map_err(Error::Nros)
	}

	/// Unmount the object store.
	///
	/// This performs one last transaction.
	pub async fn unmount(self) -> Result<Vec<D>, Error<D>> {
		self.storage.unmount().await.map_err(Error::Nros)
	}

	pub fn block_size(&self) -> BlockSize {
		self.storage.block_size()
	}

	/// Get statistics for this session.
	pub fn statistics(&self) -> Statistics {
		Statistics { object_store: self.storage.statistics() }
	}

	/// Get the key used to encrypt the header.
	pub fn header_key(&self) -> [u8; 32] {
		self.storage.header_key()
	}

	/// Set a new key derivation function.
	///
	/// This replaces the header key.
	pub fn set_key_deriver(&self, kdf: KeyDeriver<'_>) {
		self.storage.set_key_deriver(kdf)
	}

	/// Get an object.
	fn get(&self, id: u64) -> nros::Object<'_, D, nros::StdResource> {
		self.storage.get(id)
	}

	pub fn dir(&self, key: DirKey) -> Dir<'_, D> {
		Dir::new(self, key)
	}

	pub fn file(&self, key: FileKey) -> File<'_, D> {
		File::new(self, key)
	}
}

pub enum Error<D>
where
	D: Dev,
{
	Nros(nros::Error<D>),
	Truncated,
	CorruptExtension,
}

impl<D> fmt::Debug for Error<D>
where
	D: Dev,
	D::Error: fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Nros(e) => f.debug_tuple("Nros").field(e).finish(),
			Self::Truncated => f.debug_tuple("Truncated").finish(),
			Self::CorruptExtension => f.debug_tuple("CorruptExtension").finish(),
		}
	}
}

impl<D> fmt::Display for Error<D>
where
	D: Dev,
	D::Error: fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::Debug::fmt(self, f)
	}
}

impl<D> core::error::Error for Error<D>
where
	D: Dev,
	D::Error: fmt::Debug,
{
}

impl<D: Dev> From<nros::Error<D>> for Error<D> {
	fn from(err: nros::Error<D>) -> Self {
		Self::Nros(err)
	}
}

/// Statistics for this session.
///
/// Used for debugging.
#[derive(Clone, Copy, Debug, Default)]
pub struct Statistics {
	/// Object store statistics.
	pub object_store: nros::Statistics,
}
