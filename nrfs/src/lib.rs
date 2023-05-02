//#![cfg_attr(not(test), no_std)]
#![forbid(unused_must_use)]
#![forbid(elided_lifetimes_in_paths)]
#![feature(slice_as_chunks, cell_update, const_option)]
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

mod attr;
mod config;
pub mod dir;
mod file;
mod item;
#[cfg(test)]
mod test;

pub use {
	config::{LoadConfig, NewConfig},
	core::cell::RefCell,
	dir::{CreateError, Dir, RemoveError, TransferError},
	file::{File, LengthTooLong},
	item::{Item, ItemInfo, ItemKey, ItemTy},
	nrkv::Key,
	nros::{
		dev, BlockSize, CipherType, Compression, Dev, KeyDeriver, KeyPassword, MaxRecordSize,
		Resource,
	},
};

use core::{fmt, future::Future, pin::Pin};

/// NRFS filesystem manager.
#[derive(Debug)]
pub struct Nrfs<D: Dev> {
	/// Object storage.
	storage: nros::Nros<D, nros::StdResource>,
	/// Whether this filesystem is mounted as read-only.
	read_only: bool,
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

		let s = Self { storage, read_only: false };
		let id = Dir::init(&s).await?;
		s.storage.header_data()[..8].copy_from_slice(&(id << 3 | 1).to_le_bytes());

		let id = attr::AttrMap::init(&s.storage).await?;
		s.storage.header_data()[24..32].copy_from_slice(&id.to_le_bytes());

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
		Ok(Self { storage, read_only: !allow_repair })
	}

	/// Get a reference to the root directory.
	pub fn root_dir(&self) -> Dir<'_, D> {
		let data = self.storage.header_data();
		let id = u64::from_le_bytes(data[..8].try_into().unwrap()) >> 8;
		let key = ItemKey { dir: u64::MAX, tag: nrkv::Tag::MAX };
		Dir::new(self, key, id)
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

	pub async fn dir(&self, key: ItemKey) -> Result<Dir<'_, D>, Error<D>> {
		trace!("dir {:?}", key);
		let mut a = [0; 8];
		if key.dir != u64::MAX {
			let mut kv = Dir::new(self, ItemKey::INVAL, key.dir).kv().await?;
			kv.read_user_data(key.tag, 0, &mut a).await?;
		} else {
			a.copy_from_slice(&self.storage.header_data()[..8]);
		}
		assert_eq!(a[0] & 7, 1, "ty not a dir ({})", a[0] & 7);
		Ok(Dir::new(self, key, u64::from_le_bytes(a) >> 3))
	}

	pub fn file(&self, key: ItemKey) -> File<'_, D> {
		File { item: Item::new(self, key) }
	}

	pub fn item(&self, key: ItemKey) -> Item<'_, D> {
		Item::new(self, key)
	}

	pub fn resource(&self) -> &nros::StdResource {
		self.storage.resource()
	}

	pub fn max_len(&self) -> u64 {
		self.storage.obj_max_len()
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

pub(crate) struct Store<'a, D: Dev>(nros::Object<'a, D, nros::StdResource>);

impl<'a, D: Dev> nrkv::Store for Store<'a, D> {
	type Error = Error<D>;

	fn read<'s>(
		&'s mut self,
		offset: u64,
		buf: &'s mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 's>> {
		Box::pin(async move {
			self.0.read(offset, buf).await?;
			Ok(())
		})
	}

	fn write<'s>(
		&'s mut self,
		offset: u64,
		data: &'s [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 's>> {
		Box::pin(async move {
			self.0.write(offset, data).await?;
			Ok(())
		})
	}

	fn write_zeros<'s>(
		&'s mut self,
		offset: u64,
		len: u64,
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 's>> {
		Box::pin(async move {
			self.0.write_zeros(offset, len).await?;
			Ok(())
		})
	}

	fn len(&self) -> u64 {
		todo!()
	}
}
