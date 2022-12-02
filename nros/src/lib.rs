//#![cfg_attr(not(test), no_std)]
#![deny(unused_must_use)]
#![feature(int_roundings)]
#![feature(nonzero_min_max)]
#![feature(pin_macro)]

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
			pub(crate) fn from_raw(n: u8) -> Option<Self> {
				Some(match n {
					$($v => Self::$k,)*
					_ => return None,
				})
			}

			pub(crate) fn to_raw(self) -> u8 {
				self as _
			}
		}

		n2e!(@INTERNAL Shl::shl u64 $name);
		n2e!(@INTERNAL Shr::shr u64 $name);
		n2e!(@INTERNAL Shl::shl usize $name);
		n2e!(@INTERNAL Shr::shr usize $name);
	};
}

mod cache;
mod directory;
pub mod header;
mod record;
pub mod storage;
#[cfg(test)]
mod test;
mod util;

pub use {
	cache::Tree,
	record::{Compression, MaxRecordSize},
	storage::{
		dev::{Allocator, Buf},
		Dev, DevSet, Store,
	},
};

use {cache::Cache, core::fmt, record::Record, std::rc::Rc};

#[derive(Debug)]
pub struct Nros<D: Dev> {
	/// Backing store with cache and allocator.
	store: Rc<Cache<D>>,
}

impl<D: Dev> Nros<D> {
	/// Create a new object store.
	pub fn new(
		storage: DevSet<D>,
		max_record_size: MaxRecordSize,
		compression: Compression,
		cache_size: usize,
		block_size: BlockSize,
	) -> Result<Self, NewError<D>> {
		todo!()
	}

	/// Load an existing object store.
	pub async fn load(
		storage: DevSet<D>,
		read_cache_size: usize,
		write_cache_size: usize,
	) -> Result<Self, LoadError<D>> {
		todo!()
	}

	/// Create an objects.
	pub async fn create(&mut self) -> Result<u64, Error<D>> {
		self.store.clone().create().await
	}

	/// Create two objects, one at ID and one at ID + 1.
	pub async fn create_pair(&mut self) -> Result<u64, Error<D>> {
		self.store.clone().create_pair().await
	}

	/// Decrement the reference count to an object.
	///
	/// If this count reaches zero the object is automatically freed.
	///
	/// This function *must not* be used on invalid objects!
	pub async fn decr_ref(&mut self, id: u64) -> Result<(), Error<D>> {
		self.store.clone().decrease_refcount(id).await
	}

	pub async fn move_object(&mut self, to_id: u64, from_id: u64) -> Result<(), Error<D>> {
		self.store.clone().move_object(from_id, to_id).await
	}

	pub async fn finish_transaction(&self) -> Result<(), Error<D>> {
		self.store.finish_transaction().await
	}

	pub fn block_size(&self) -> BlockSize {
		self.store.block_size()
	}

	/// Return an owned reference to an object.
	///
	/// # Note
	///
	/// This does not check if the object is valid.
	pub fn get(&self, id: u64) -> Tree<D> {
		self.store.clone().get(id)
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
