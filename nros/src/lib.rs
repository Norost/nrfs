//#![cfg_attr(not(test), no_std)]
#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]
#![feature(stmt_expr_attributes)]
#![feature(no_coverage)]
#![feature(map_many_mut)]
#![feature(is_some_and)]
#![feature(async_closure)]
#![feature(never_type)]
#![feature(get_many_mut)]
#![feature(cell_update)]
#![feature(hash_drain_filter, btree_drain_filter)]
#![feature(int_roundings)]
#![feature(iterator_try_collect)]
#![feature(map_try_insert)]
#![feature(nonzero_min_max)]
#![feature(slice_flatten)]
#![feature(type_alias_impl_trait)]
#![feature(error_in_core)]

extern crate alloc;

macro_rules! raw {
	($ty:ty) => {
		impl AsRef<[u8; core::mem::size_of::<Self>()]> for $ty {
			fn as_ref(&self) -> &[u8; core::mem::size_of::<Self>()] {
				unsafe { &*(self as *const _ as *const _) }
			}
		}

		impl AsMut<[u8; core::mem::size_of::<Self>()]> for $ty {
			fn as_mut(&mut self) -> &mut [u8; core::mem::size_of::<Self>()] {
				unsafe { &mut *(self as *mut _ as *mut _) }
			}
		}
	};
}

macro_rules! n2e {
	{
		$(#[doc = $doc:literal])*
		[$name:ident]
		$($v:literal $k:ident)*
	} => {
		$(#[doc = $doc])*
		#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
		pub enum $name {
			$($k = $v,)*
		}

		impl $name {
			pub fn from_raw(n: u8) -> Option<Self> {
				Some(match n {
					$($v => Self::$k,)*
					_ => return None,
				})
			}

			pub fn to_raw(self) -> u8 {
				self as _
			}
		}
	};
}

/// Tracing in debug mode only.
macro_rules! trace {
	(info $($arg:tt)*) => {{
		let f = #[no_coverage]|| if cfg!(feature = "trace") {
			$crate::trace::print_debug("--> ", &format_args!($($arg)*));
		};
		f();
	}};
	(final $($arg:tt)*) => {{
		let f = #[no_coverage]|| if cfg!(feature = "trace") {
			$crate::trace::print_debug("==> ", &format_args!($($arg)*));
		};
		f();
	}};
	($($arg:tt)*) => {
		let f = #[no_coverage]|| if cfg!(feature = "trace") {
			$crate::trace::print_debug("", &format_args!($($arg)*));
		};
		f();
		let _t = $crate::trace::Trace::new();
	};
}

#[cfg(not(feature = "trace"))]
mod trace {
	use core::fmt::Arguments;

	pub struct Trace;

	impl Trace {
		#[inline(always)]
		#[no_coverage]
		pub fn new() {}
	}

	#[inline(always)]
	#[no_coverage]
	pub fn print_debug(_prefix: &str, _args: &Arguments<'_>) {}
}

mod background;
mod block_size;
mod cache;
mod cipher;
mod config;
mod header;
mod key_derivation;
mod object;
mod record;
pub mod resource;
mod storage;
#[cfg(any(test, fuzzing))]
pub mod test;
#[cfg(feature = "trace")]
mod trace;
mod util;
mod waker_queue;

#[cfg(not(no_std))]
pub use resource::StdResource;
pub use {
	block_size::BlockSize,
	cache::{Statistics, Tree},
	cipher::CipherType,
	config::{KeyDeriver, KeyPassword, LoadConfig, NewConfig},
	record::{Compression, MaxRecordSize},
	resource::Resource,
	storage::{dev, Dev},
};

use {
	cache::Cache,
	cipher::{Cipher, HeaderCipher, RecordCipher},
	core::{fmt, future::Future},
	key_derivation::KeyDerivation,
	record::Record,
	storage::{DevSet, Store},
	waker_queue::{WakerQueue, WakerQueueTicket},
};

type Background<'a, D> = background::Background<'a, Result<(), Error<D>>>;

#[derive(Debug)]
pub struct Nros<D: Dev, R: Resource> {
	/// Backing store with cache and allocator.
	store: Cache<D, R>,
}

impl<D: Dev, R: Resource> Nros<D, R> {
	/// Create a new object store.
	pub async fn new(config: NewConfig<'_, D, R>) -> Result<Self, Error<D>> {
		let cache_size = config.cache_size;
		let devs = DevSet::new(config).await?;
		Self::load_inner(devs, cache_size, true).await
	}

	/// Load an existing object store.
	pub async fn load(config: LoadConfig<'_, D, R>) -> Result<Self, Error<D>> {
		let cache_size = config.cache_size;
		let allow_repair = config.allow_repair;
		let devs = DevSet::load(config).await?;
		Self::load_inner(devs, cache_size, allow_repair).await
	}

	/// Load an object store.
	async fn load_inner(
		devices: DevSet<D, R>,
		cache_size: usize,
		allow_repair: bool,
	) -> Result<Self, Error<D>> {
		let store = Store::new(devices, allow_repair).await?;
		let store = Cache::new(store, cache_size).await?;
		Ok(Self { store })
	}

	/// Start running tasks.
	///
	/// Background tasks will run concurrently.
	pub async fn run<V, E, F>(&self, f: F) -> Result<V, E>
	where
		F: Future<Output = Result<V, E>>,
		E: From<Error<D>>,
	{
		self.store.run(f).await
	}

	/// Create an object.
	pub async fn create(&self) -> Result<Tree<'_, D, R>, Error<D>> {
		self.store.create().await
	}

	/// Create multiple adjacent objects, from ID up to ID + N - 1.
	pub async fn create_many(&self, amount: u64) -> Result<u64, Error<D>> {
		self.store.create_many(amount).await
	}

	pub async fn finish_transaction<'a>(&'a self) -> Result<(), Error<D>> {
		self.store.finish_transaction().await
	}

	pub fn block_size(&self) -> BlockSize {
		self.store.block_size()
	}

	/// Return an owned reference to an object.
	pub async fn get(&self, id: u64) -> Result<Tree<'_, D, R>, Error<D>> {
		assert!(
			id != u64::MAX,
			"ID u64::MAX is reserved for the object list"
		);
		self.store.get(id).await
	}

	/// Readjust cache size.
	///
	/// This may be useful to increase or decrease depending on total system memory usage.
	///
	/// # Panics
	///
	/// If `global_max < write_max`.
	pub fn resize_cache(&self, soft_limit: usize) -> Result<(), Error<D>> {
		self.store.resize_cache(soft_limit)
	}

	/// Get statistics for current session.
	pub fn statistics(&self) -> Statistics {
		self.store.statistics()
	}

	/// Unmount the object store.
	///
	/// This performs one last transaction.
	pub async fn unmount(self) -> Result<Vec<D>, Error<D>> {
		let store = self.store.unmount().await?;
		let devset = store.unmount().await?;
		Ok(devset.into_devices())
	}

	/// Get the key used to encrypt the header.
	pub fn header_key(&self) -> [u8; 32] {
		self.store.header_key()
	}

	/// Set a new key derivation function.
	///
	/// This replaces the header key.
	pub fn set_key_deriver(&self, kdf: KeyDeriver<'_>) {
		self.store.set_key_deriver(kdf)
	}

	/// The unique identifier for this filesystem.
	pub fn uid(&self) -> u128 {
		todo!()
		//self.store
	}
}

pub enum NewError<D: Dev> {
	BlockTooSmall,
	Dev(D::Error),
}

#[derive(Debug)]
pub enum LoadError<D: Dev> {
	InvalidMagic,
	InvalidRecordSize(u8),
	UnsupportedCompression(u8),
	Dev(D::Error),
}

pub enum Error<D: Dev> {
	Dev(D::Error),
	RecordUnpack(record::UnpackError),
	NotEnoughSpace,
}

impl<D: Dev> fmt::Debug for NewError<D>
where
	D::Error: fmt::Debug,
{
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::BlockTooSmall => f.debug_tuple("BlockTooSmall").finish(),
			Self::Dev(e) => f.debug_tuple("Dev").field(&e).finish(),
		}
	}
}

impl<D: Dev> fmt::Debug for Error<D>
where
	D::Error: fmt::Debug,
{
	#[no_coverage]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Dev(e) => f.debug_tuple("Dev").field(&e).finish(),
			Self::RecordUnpack(e) => f.debug_tuple("RecordUnpack").field(&e).finish(),
			Self::NotEnoughSpace => f.debug_tuple("NotEnoughSpace").finish(),
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
