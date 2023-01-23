//#![cfg_attr(not(test), no_std)]
#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]
#![feature(map_many_mut)]
#![feature(is_some_and)]
#![feature(async_closure)]
#![feature(never_type)]
#![feature(get_many_mut)]
#![feature(cell_update)]
#![feature(hash_drain_filter)]
#![feature(int_roundings)]
#![feature(iterator_try_collect)]
#![feature(map_try_insert)]
#![feature(nonzero_min_max)]
#![feature(pin_macro)]
#![feature(slice_flatten)]
#![feature(type_alias_impl_trait)]

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
	(@INTERNAL $op:ident :: $fn:ident $int:ident $name:ident) => {
		impl core::ops::$op<$name> for $int {
			type Output = $int;

			fn $fn(self, rhs: $name) -> Self::Output {
				self.$fn(rhs.to_raw())
			}
		}
	};
	{
		$(#[doc = $doc:literal])*
		[$name:ident]
		$($v:literal $k:ident)*
	} => {
		$(#[doc = $doc])*
		#[derive(Clone, Copy, Debug)]
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

		n2e!(@INTERNAL Shl::shl u64 $name);
		n2e!(@INTERNAL Shr::shr u64 $name);
		n2e!(@INTERNAL Shl::shl usize $name);
		n2e!(@INTERNAL Shr::shr usize $name);
	};
}

/// Tracing in debug mode only.
macro_rules! trace {
	($($arg:tt)*) => {
		#[cfg(feature = "trace")]
		$crate::trace::print_debug(&format_args!($($arg)*));
		let _t = $crate::trace::Trace::new();
	};
}

#[cfg(not(feature = "trace"))]
mod trace {
	pub struct Trace;

	impl Trace {
		#[inline(always)]
		pub fn new() {}
	}
}

#[cfg(feature = "trace")]
mod trace {
	use core::{cell::Cell, fmt::Arguments};

	thread_local! {
		static DEPTH: Cell<usize> = Cell::new(0);
	}

	pub fn print_debug(args: &Arguments<'_>) {
		DEPTH.with(|depth| {
			eprintln!("[nros]{:>pad$} {}", "", args, pad = depth.get() * 2);
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

mod background;
mod cache;
mod header;
mod record;
pub mod resource;
mod storage;
#[cfg(any(test, fuzzing))]
pub mod test;
mod util;

#[cfg(not(no_std))]
pub use resource::StdResource;
pub use {
	cache::{Statistics, Tree},
	record::{Compression, MaxRecordSize},
	resource::Resource,
	storage::{dev, Dev, Store},
};

use {
	cache::Cache,
	core::{fmt, future::Future, pin::Pin},
	record::Record,
	storage::DevSet,
};

pub type Background<'a, D> =
	background::Background<Pin<Box<dyn Future<Output = Result<(), Error<D>>> + 'a>>>;

#[derive(Debug)]
pub struct Nros<D: Dev, R: Resource> {
	/// Backing store with cache and allocator.
	store: Cache<D, R>,
}

impl<D: Dev, R: Resource> Nros<D, R> {
	/// Create a new object store.
	pub async fn new<M, C>(
		resource: R,
		mirrors: M,
		block_size: BlockSize,
		max_record_size: MaxRecordSize,
		compression: Compression,
		read_cache_size: usize,
	) -> Result<Self, Error<D>>
	where
		M: IntoIterator<Item = C>,
		C: IntoIterator<Item = D>,
	{
		let devs = DevSet::new(resource, mirrors, block_size, max_record_size, compression).await?;
		Self::load_inner(devs, read_cache_size, true).await
	}

	/// Load an existing object store.
	pub async fn load(
		resource: R,
		devices: Vec<D>,
		read_cache_size: usize,
		allow_repair: bool,
	) -> Result<Self, Error<D>> {
		let devs = DevSet::load(resource, devices, allow_repair).await?;
		Self::load_inner(devs, read_cache_size, allow_repair).await
	}

	/// Load an object store.
	pub async fn load_inner(
		devices: DevSet<D, R>,
		read_cache_size: usize,
		allow_repair: bool,
	) -> Result<Self, Error<D>> {
		let store = Store::new(devices, allow_repair).await?;
		let store = Cache::new(store, read_cache_size);
		Ok(Self { store })
	}

	/// Create an object.
	pub async fn create<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
	) -> Result<Tree<'a, 'b, D, R>, Error<D>> {
		self.store.create(bg).await
	}

	/// Create multiple adjacent objects, from ID up to ID + N - 1.
	pub async fn create_many<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		amount: u64,
	) -> Result<u64, Error<D>> {
		self.store.create_many(bg, amount).await
	}

	pub async fn finish_transaction<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
	) -> Result<(), Error<D>> {
		self.store.finish_transaction(bg).await
	}

	pub fn block_size(&self) -> BlockSize {
		self.store.block_size()
	}

	/// Return an owned reference to an object.
	pub async fn get<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
		id: u64,
	) -> Result<Tree<'a, 'b, D, R>, Error<D>> {
		assert!(
			id != u64::MAX,
			"ID u64::MAX is reserved for the object list"
		);
		self.store.get(bg, id).await
	}

	/// Readjust cache size.
	///
	/// This may be useful to increase or decrease depending on total system memory usage.
	///
	/// # Panics
	///
	/// If `global_max < write_max`.
	pub async fn resize_cache<'a>(
		&'a self,
		bg: &Background<'a, D>,
		global_max: usize,
	) -> Result<(), Error<D>> {
		self.store.resize_cache(bg, global_max).await
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
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Dev(e) => f.debug_tuple("Dev").field(&e).finish(),
			Self::RecordUnpack(e) => f.debug_tuple("RecordUnpack").field(&e).finish(),
			Self::NotEnoughSpace => f.debug_tuple("NotEnoughSpace").finish(),
		}
	}
}

n2e! {
	[BlockSize]
	9 B512
	10 K1
	11 K2
	12 K4
	13 K8
	14 K16
	15 K32
	16 K64
	17 K128
	18 K256
	19 K512
	20 M1
	21 M2
	22 M4
	23 M8
	24 M16
	25 M32
	26 M64
	27 M128
	28 M256
	29 M512
	30 G1
	31 G2
}

impl<D: Dev> From<!> for Error<D> {
	fn from(x: !) -> Self {
		x
	}
}
