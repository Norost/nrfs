//#![cfg_attr(not(test), no_std)]
#![forbid(unused_must_use)]
#![forbid(elided_lifetimes_in_paths)]
#![feature(cell_update)]
#![feature(pin_macro)]
#![feature(split_array)]

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

pub mod dir;
mod file;
mod name;
#[cfg(test)]
mod test;

pub use {
	dir::DirOptions,
	name::Name,
	nros::{dev, BlockSize, Compression, Dev, MaxRecordSize},
};

use {
	core::{
		cell::{RefCell, RefMut},
		fmt,
	},
	dir::{Child, DirData, Entry},
	file::FileData,
	rustc_hash::FxHashMap,
	std::collections::hash_map,
};

/// Index used for arenas with file data.
type Idx = arena::Handle<u8>;

/// [`Nrfs`] shared mutable data.
#[derive(Debug, Default)]
struct NrfsData {
	/// Files with live references.
	///
	/// Since filess may be embedded at any time using IDs is not practical.
	files: arena::Arena<FileData, u8>,
	/// Directories with live references.
	///
	/// Indexed by ID.
	directories: FxHashMap<u64, DirData>,
	/// Entries that were destroyed but still have live references.
	///
	/// The value indicates the amount of references remaining.
	destroyed: FxHashMap<Child, usize>,
}

impl NrfsData {
	/// Remove a reference to a parent.
	fn remove_parent_reference(&mut self) {}
}

/// NRFS filesystem manager.
#[derive(Debug)]
pub struct Nrfs<D: Dev> {
	/// Object storage.
	storage: nros::Nros<D>,
	/// Data of objects with live references.
	data: RefCell<NrfsData>,
}

impl<D: Dev> Nrfs<D> {
	pub async fn new<M, C>(
		mirrors: M,
		block_size: BlockSize,
		max_record_size: MaxRecordSize,
		dir: &DirOptions,
		compression: Compression,
		global_cache_size: usize,
		dirty_cache_size: usize,
	) -> Result<Self, Error<D>>
	where
		M: IntoIterator<Item = C>,
		C: IntoIterator<Item = D>,
	{
		let storage = nros::Nros::new(
			mirrors,
			block_size,
			max_record_size,
			compression,
			global_cache_size,
			dirty_cache_size,
		)
		.await?;
		let mut s = Self { storage, data: Default::default() };
		DirRef::new_root(&mut s, dir).await?;
		Ok(s)
	}

	pub async fn load(
		devices: Vec<D>,
		global_cache_size: usize,
		dirty_cache_size: usize,
	) -> Result<Self, Error<D>> {
		Ok(Self {
			storage: nros::Nros::load(devices, global_cache_size, dirty_cache_size).await?,
			data: Default::default(),
		})
	}

	/// Get a reference to the root directory.
	pub async fn root_dir(&self) -> Result<DirRef<'_, D>, Error<D>> {
		DirRef::load_root(self).await
	}

	pub async fn finish_transaction(&self) -> Result<(), Error<D>> {
		self.storage.finish_transaction().await.map_err(Error::Nros)
	}

	async fn read(&self, id: u64, offset: u64, buf: &mut [u8]) -> Result<usize, Error<D>> {
		self.storage
			.get(id)
			.await?
			.read(offset, buf)
			.await
			.map_err(Error::Nros)
	}

	async fn read_exact(&self, id: u64, offset: u64, buf: &mut [u8]) -> Result<(), Error<D>> {
		self.read(id, offset, buf)
			.await
			.and_then(|l| (l == buf.len()).then_some(()).ok_or(Error::Truncated))
	}

	async fn write(&self, id: u64, offset: u64, data: &[u8]) -> Result<usize, Error<D>> {
		self.storage
			.get(id)
			.await?
			.write(offset, data)
			.await
			.map_err(Error::Nros)
	}

	async fn write_all(&self, id: u64, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		self.write(id, offset, data)
			.await
			.and_then(|l| (l == data.len()).then_some(()).ok_or(Error::Truncated))
	}

	/// This function automatically grows the object if it can't contain the data.
	async fn write_grow(&self, id: u64, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		if self.length(id).await? < offset + data.len() as u64 {
			self.resize(id, offset + data.len() as u64).await?;
		}
		self.write_all(id, offset, data).await
	}

	async fn resize(&self, id: u64, len: u64) -> Result<(), Error<D>> {
		self.storage
			.get(id)
			.await?
			.resize(len)
			.await
			.map_err(Error::Nros)
	}

	/// Get the length of an object.
	async fn length(&self, id: u64) -> Result<u64, Error<D>> {
		self.storage.get(id).await?.len().await.map_err(Error::Nros)
	}

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

	/// Get a reference to a [`FileData`] structure.
	fn file_data(&self, idx: Idx) -> RefMut<'_, FileData> {
		RefMut::map(self.data.borrow_mut(), |fs| &mut fs.files[idx])
	}

	/// Get a reference to a [`DirData`] structure.
	fn dir_data(&self, id: u64) -> RefMut<'_, DirData> {
		RefMut::map(self.data.borrow_mut(), |fs| {
			fs.directories.get_mut(&id).expect("no DirData with id")
		})
	}
}

/// Reference to a directory object.
#[derive(Debug)]
pub struct DirRef<'a, D: Dev> {
	/// Filesystem object containing the directory.
	fs: &'a Nrfs<D>,
	/// ID of the directory object.
	id: u64,
}

/// Raw [`DirRef`] data.
///
/// This is more compact than [`DirRef`] and better suited for storing in a container.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawDirRef {
	/// ID of the directory object.
	id: u64,
}

impl<'a, D: Dev> DirRef<'a, D> {
	/// Turn this reference into raw components.
	fn into_raw(self) -> RawDirRef {
		let Self { fs: _, id } = self;
		RawDirRef { id }
	}

	/// Create a reference from raw components.
	fn from_raw(fs: &'a Nrfs<D>, raw: RawDirRef) -> Self {
		let RawDirRef { id } = raw;
		DirRef { fs, id }
	}
}

impl<'a, D: Dev> Clone for DirRef<'a, D> {
	fn clone(&self) -> Self {
		self.fs.dir_data(self.id).header.reference_count += 1;
		Self { fs: self.fs, id: self.id }
	}
}

impl<D: Dev> Drop for DirRef<'_, D> {
	fn drop(&mut self) {
		let mut fs = self.fs.data.borrow_mut();
		let hash_map::Entry::Occupied(mut data) = fs.directories.entry(self.id) else {
			unreachable!()
		};
		data.get_mut().header.reference_count -= 1;
		if data.get().header.reference_count == 0 {
			// Remove DirData.
			let DataHeader { parent_id, parent_index, .. } = data.get().header;
			data.remove();

			// If this is the root dir there is no parent dir,
			// so check first.
			if self.id != 0 {
				// Remove itself from parent directory.
				let dir = fs
					.directories
					.get_mut(&parent_id)
					.expect("parent dir is not loaded");
				let _r = dir.children.remove(&parent_index);
				debug_assert!(
					matches!(_r, Some(Child::Dir(id)) if id == self.id),
					"child not present in parent"
				);

				// Reconstruct DirRef to adjust reference count of dir appropriately.
				drop(fs);
				drop(DirRef { fs: self.fs, id: parent_id });
			}
		}
	}
}

/// Reference to a file object.
#[derive(Debug)]
pub struct FileRef<'a, D: Dev> {
	/// Filesystem object containing the directory.
	fs: &'a Nrfs<D>,
	/// Handle pointing to the corresponding [`FileData`].
	idx: Idx,
}

/// Raw [`FileRef`] data.
///
/// This is more compact than [`FileRef`] and better suited for storing in a container.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawFileRef {
	/// Handle pointing to the corresponding [`FileData`].
	idx: Idx,
}

impl<'a, D: Dev> FileRef<'a, D> {
	/// Turn this reference into raw components.
	fn into_raw(self) -> RawFileRef {
		let Self { fs: _, idx } = self;
		RawFileRef { idx }
	}

	/// Create a reference from raw components.
	fn from_raw(fs: &'a Nrfs<D>, raw: RawFileRef) -> Self {
		let RawFileRef { idx } = raw;
		FileRef { fs, idx }
	}
}

impl<'a, D: Dev> Clone for FileRef<'a, D> {
	fn clone(&self) -> Self {
		self.fs.file_data(self.idx).header.reference_count += 1;
		Self { fs: self.fs, idx: self.idx }
	}
}

impl<D: Dev> Drop for FileRef<'_, D> {
	fn drop(&mut self) {
		let mut fs_ref = self.fs.data.borrow_mut();
		let fs = &mut *fs_ref; // borrow errors ahoy!

		let mut data = fs
			.files
			.get_mut(self.idx)
			.expect("filedata should be present");

		data.header.reference_count -= 1;
		if data.header.reference_count == 0 {
			// Remove itself from parent directory.
			let dir = fs
				.directories
				.get_mut(&data.header.parent_id)
				.expect("parent dir is not loaded");
			let _r = dir.children.remove(&data.header.parent_index);
			debug_assert!(matches!(_r, Some(Child::File(idx)) if idx == self.idx));

			// Remove filedata.
			let data = fs
				.files
				.remove(self.idx)
				.expect("filedata should be present");

			// Reconstruct DirRef to adjust reference count of dir appropriately.
			drop(fs_ref);
			drop(DirRef { fs: self.fs, id: data.header.parent_id });
		}
	}
}

/// Reference to a file object representing a symbolic link.
#[derive(Clone, Debug)]
pub struct SymRef<'a, D: Dev>(FileRef<'a, D>);

/// Raw [`SymRef`] data.
///
/// This is more compact than [`SymRef`] and better suited for storing in a container.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawSymRef(RawFileRef);

impl<'a, D: Dev> SymRef<'a, D> {
	/// Turn this reference into raw components.
	fn into_raw(self) -> RawSymRef {
		RawSymRef(self.0.into_raw())
	}

	/// Create a reference from raw components.
	fn from_raw(fs: &'a Nrfs<D>, raw: RawSymRef) -> Self {
		SymRef(FileRef::from_raw(fs, raw.0))
	}
}

/// Reference to an entry with an unrecognized type.
#[derive(Clone, Debug)]
pub struct UnknownRef<'a, D: Dev>(FileRef<'a, D>);

/// Raw [`UnknownRef`] data.
///
/// This is more compact than [`UnknownRef`] and better suited for storing in a container.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawUnknownRef(RawFileRef);

impl<'a, D: Dev> UnknownRef<'a, D> {
	/// Turn this reference into raw components.
	fn into_raw(self) -> RawUnknownRef {
		RawUnknownRef(self.0.into_raw())
	}

	/// Create a reference from raw components.
	fn from_raw(fs: &'a Nrfs<D>, raw: RawUnknownRef) -> Self {
		UnknownRef(FileRef::from_raw(fs, raw.0))
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

/// Write an exact amount of data.
///
/// Fails if not all data could be written.
async fn write_all<'a, D: Dev>(
	obj: &nros::Tree<'a, D>,
	offset: u64,
	data: &[u8],
) -> Result<(), Error<D>> {
	let l = obj.write(offset, data).await?;
	(l == data.len()).then_some(()).ok_or(Error::Truncated)
}

/// Read an exact amount of data.
///
/// Fails if the buffer could not be filled.
async fn read_exact<'a, D: Dev>(
	obj: &nros::Tree<'a, D>,
	offset: u64,
	buf: &mut [u8],
) -> Result<(), Error<D>> {
	let l = obj.read(offset, buf).await?;
	(l == buf.len()).then_some(()).ok_or(Error::Truncated)
}

/// Data header, shared by [`DirData`] and [`FileData`].
#[derive(Debug)]
struct DataHeader {
	/// The amount of live [`DirRef`]s to this directory.
	reference_count: usize,
	/// ID of the parent directory.
	///
	/// Not applicable if the ID of the object is 0,
	/// i.e. it is the root directory.
	parent_id: u64,
	/// Index in the parent directory.
	///
	/// Not applicable if the ID of the object is 0,
	/// i.e. it is the root directory.
	parent_index: u32,
}

impl DataHeader {
	/// Create a new header.
	fn new(parent_id: u64, parent_index: u32) -> Self {
		Self { reference_count: 1, parent_id, parent_index }
	}
}
