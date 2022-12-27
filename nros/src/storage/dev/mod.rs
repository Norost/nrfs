//! # Storage device interface
//!
//! This module provides an interface for abstracting a storage device.
//! It is designed to support asynchronous operations.
//!
//! Devices don't provide memory buffers directly.
//! Instead, a separate object is used which may provide buffers for multiple
//! devices at once.

#[cfg(not(feature = "no-std"))]
mod fs;
mod mem;
mod set;

#[cfg(not(feature = "no-std"))]
pub use fs::{FileDev, FileDevError};
pub use {
	mem::{MemDev, MemDevError},
	set::DevSet,
};

use {crate::BlockSize, core::future::Future};

pub trait Dev {
	/// Error that may be returned by the device.
	///
	/// This should only be used for fatal errors.
	/// The device will be taken out of service as soon as an error is returned.
	type Error;
	/// The type of the memory allocator used by this device.
	///
	/// This allocator is used by [`Nros`] for write buffers and by [`Dev`]ices for read buffers.
	type Allocator: Allocator<Error = Self::Error>;
	/// Task that represents a pending read operation.
	type ReadTask<'a>: Future<Output = Result<<Self::Allocator as Allocator>::Buf<'a>, Self::Error>>
	where
		Self: 'a;
	/// Task that represents a pending write operation.
	///
	/// This task may finish before the data has been flushed.
	type WriteTask<'a>: Future<Output = Result<(), Self::Error>>
	where
		Self: 'a;
	/// Task that represents a pending fence operation.
	type FenceTask<'a>: Future<Output = Result<(), Self::Error>>
	where
		Self: 'a;

	/// The amount of useable blocks.
	fn block_count(&self) -> u64;

	/// The size of a block.
	fn block_size(&self) -> BlockSize;

	/// Read a range of blocks.
	fn read(&self, lba: u64, len: usize) -> Self::ReadTask<'_>;

	/// Write data.
	///
	/// The range can be used to write chunks of a buffer to multiple devices without redundant
	/// copying.
	///
	/// # Panics
	///
	/// If `len > buf.len()`.
	///
	/// If a fence is in progress.
	fn write(&self, lba: u64, buf: <Self::Allocator as Allocator>::Buf<'_>) -> Self::WriteTask<'_>;

	/// Execute a fence.
	///
	/// This operation finishes when all previous writes have finished,
	/// i.e. all changed have been flushed to non-volatile storage.
	///
	/// # Panics
	///
	/// If a fence is already in progress.
	fn fence(&self) -> Self::FenceTask<'_>;

	/// Get the memory allocator used by this device.
	/// Used to allocate buffers for writing.
	fn allocator(&self) -> &Self::Allocator;
}

/// Interface for allocators which manage memory buffers [`Dev`] can read from & write to.
pub trait Allocator {
	type Buf<'a>: Buf
	where
		Self: 'a;
	type Error;

	/// Task that represents a pending allocation.
	type AllocTask<'a>: Future<Output = Result<Self::Buf<'a>, Self::Error>>
	where
		Self: 'a;

	/// Allocate a buffer.
	/// `size` is in bytes.
	///
	/// The returned buffer **must** have a unique reference to its storage.
	fn alloc(&self, size: usize) -> Self::AllocTask<'_>;
}

/// A memory buffer for use with [`Dev`].
pub trait Buf: Clone {
	/// Error that may occur when implicitly cloning.
	type Error;

	/// Get an immutable reference to the buffer.
	fn get(&self) -> &[u8];

	/// Get a mutable reference to the buffer.
	///
	/// This may only be called if no copies have been
	///
	/// # Panics
	///
	/// If [`Self::deep_clone`] was called on this buffer.
	fn get_mut(&mut self) -> &mut [u8];

	/// Shrink the buffer.
	fn shrink(&mut self, len: usize);

	/// The length of this buffer, in bytes.
	fn len(&self) -> usize {
		self.get().len()
	}
}
