//#![cfg_attr(not(test), no_std)]
#![forbid(unused_must_use)]
#![forbid(elided_lifetimes_in_paths)]
#![feature(iterator_try_collect)]
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
#[cfg(any(test, fuzzing))]
pub mod test;

pub use {
	dir::DirOptions,
	name::Name,
	nros::{dev, BlockSize, Compression, Dev, MaxRecordSize},
};

use {
	core::{
		cell::{RefCell, RefMut},
		fmt,
		marker::PhantomData,
		mem,
		ops::{Deref, DerefMut},
	},
	dir::{Child, DirData, ItemRef},
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
}

/// NRFS filesystem manager.
#[derive(Debug)]
pub struct Nrfs<D: Dev> {
	/// Object storage.
	storage: nros::Nros<D>,
	/// Data of objects with live references.
	data: RefCell<NrfsData>,
	/// Whether this filesystem is mounted as read-only.
	read_only: bool,
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
		let mut s = Self { storage, data: Default::default(), read_only: false };
		DirRef::new_root(&mut s, dir).await?.drop().await?;
		Ok(s)
	}

	/// `read_only` guarantees no modifications will be made.
	// TODO read_only is a sham.
	pub async fn load(
		devices: Vec<D>,
		global_cache_size: usize,
		dirty_cache_size: usize,
		read_only: bool,
	) -> Result<Self, Error<D>> {
		Ok(Self {
			storage: nros::Nros::load(devices, global_cache_size, dirty_cache_size).await?,
			data: Default::default(),
			read_only,
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
		let obj = self.storage.get(id).await?;
		Ok(read_exact(&obj, offset, buf).await?)
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
		let obj = self.storage.get(id).await?;
		Ok(write_all(&obj, offset, data).await?)
	}

	/// This function automatically grows the object if it can't contain the data.
	async fn write_grow(&self, id: u64, offset: u64, data: &[u8]) -> Result<(), Error<D>> {
		let obj = self.storage.get(id).await?;
		if obj.len().await? < offset + data.len() as u64 {
			obj.resize(offset + data.len() as u64).await?;
		}
		write_all(&obj, offset, data).await
	}

	async fn resize(&self, id: u64, len: u64) -> Result<(), Error<D>> {
		self.storage
			.get(id)
			.await?
			.resize(len)
			.await
			.map_err(Error::Nros)
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

/// Trait to convert between "raw" and "complete" references,
/// i.e. references without direct access to the filesystem
/// and references with.
pub trait RawRef<'a, D: Dev>: Sized + 'a {
	/// The type of the raw reference.
	type Raw;

	/// Turn this reference into raw components.
	fn into_raw(self) -> Self::Raw {
		mem::ManuallyDrop::new(self).as_raw()
	}

	/// Get a raw reference.
	fn as_raw(&self) -> Self::Raw;

	/// Create a reference from raw components.
	///
	/// *Must* only be used in combination with [`Self::into_raw`]!
	fn from_raw(fs: &'a Nrfs<D>, raw: Self::Raw) -> Self;
}

/// Reference to a directory object.
#[derive(Debug)]
#[must_use = "Must be manually dropped with DirRef::drop"]
pub struct DirRef<'a, D: Dev> {
	/// Filesystem object containing the directory.
	fs: &'a Nrfs<D>,
	/// ID of the directory object.
	id: u64,
}

impl<'a, D: Dev> DirRef<'a, D> {}

/// Raw [`DirRef`] data.
///
/// This is more compact than [`DirRef`] and better suited for storing in a container.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use = "value must be used to avoid reference leaks"]
pub struct RawDirRef {
	/// ID of the directory object.
	id: u64,
}

impl<'a, D: Dev> RawRef<'a, D> for DirRef<'a, D> {
	type Raw = RawDirRef;

	fn as_raw(&self) -> RawDirRef {
		RawDirRef { id: self.id }
	}

	fn from_raw(fs: &'a Nrfs<D>, raw: RawDirRef) -> Self {
		Self { fs, id: raw.id }
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
		forbid_drop()
	}
}

/// Reference to a file object.
#[derive(Debug)]
#[must_use = "Must be manually dropped with FileRef::drop"]
pub struct FileRef<'a, D: Dev> {
	/// Filesystem object containing the directory.
	fs: &'a Nrfs<D>,
	/// Handle pointing to the corresponding [`FileData`].
	idx: Idx,
}

/// Raw [`FileRef`] data.
///
/// This is more compact than [`FileRef`] and better suited for storing in a container.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use = "value must be used to avoid reference leaks"]
pub struct RawFileRef {
	/// Handle pointing to the corresponding [`FileData`].
	idx: Idx,
}

impl<'a, D: Dev> RawRef<'a, D> for FileRef<'a, D> {
	type Raw = RawFileRef;

	fn as_raw(&self) -> RawFileRef {
		RawFileRef { idx: self.idx }
	}

	fn from_raw(fs: &'a Nrfs<D>, raw: RawFileRef) -> Self {
		Self { fs, idx: raw.idx }
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
		forbid_drop()
	}
}

/// Reference to a file object representing a symbolic link.
#[derive(Clone, Debug)]
#[must_use = "Must be manually dropped with SymRef::drop"]
pub struct SymRef<'a, D: Dev>(FileRef<'a, D>);

/// Raw [`SymRef`] data.
///
/// This is more compact than [`SymRef`] and better suited for storing in a container.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use = "value must be used to avoid reference leaks"]
pub struct RawSymRef(RawFileRef);

impl<'a, D: Dev> RawRef<'a, D> for SymRef<'a, D> {
	type Raw = RawSymRef;

	fn as_raw(&self) -> RawSymRef {
		RawSymRef(self.0.as_raw())
	}

	fn from_raw(fs: &'a Nrfs<D>, raw: RawSymRef) -> Self {
		SymRef(FileRef::from_raw(fs, raw.0))
	}
}

/// Reference to an entry with an unrecognized type.
#[derive(Clone, Debug)]
#[must_use = "Must be manually dropped with UnknownRef::drop"]
pub struct UnknownRef<'a, D: Dev>(FileRef<'a, D>);

/// Raw [`UnknownRef`] data.
///
/// This is more compact than [`UnknownRef`] and better suited for storing in a container.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use = "value must be used to avoid reference leaks"]
pub struct RawUnknownRef(RawFileRef);

impl<'a, D: Dev> RawRef<'a, D> for UnknownRef<'a, D> {
	type Raw = RawUnknownRef;

	fn as_raw(&self) -> RawUnknownRef {
		RawUnknownRef(self.0.as_raw())
	}

	fn from_raw(fs: &'a Nrfs<D>, raw: RawUnknownRef) -> Self {
		UnknownRef(FileRef::from_raw(fs, raw.0))
	}
}

/// "temporary" reference, i.e reference that doesn't run its destructor on drop.
pub struct TmpRef<'a, T> {
	inner: mem::ManuallyDrop<T>,
	_marker: PhantomData<&'a ()>,
}

impl<'a, T> Deref for TmpRef<'a, T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl<'a, T> DerefMut for TmpRef<'a, T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.inner
	}
}

macro_rules! impl_tmpref {
	($raw:ident $ref:ident $var:ident) => {
		impl $raw {
			/// Create a "temporary" reference.
			///
			/// See [`TmpRef`] for more information.
			pub fn into_tmp<'s, 'f, D: Dev>(&'s self, fs: &'f Nrfs<D>) -> TmpRef<'s, $ref<'f, D>> {
				TmpRef {
					inner: mem::ManuallyDrop::new($ref::from_raw(fs, self.clone())),
					_marker: PhantomData,
				}
			}
		}

		impl<'s, 'f, D: Dev> From<TmpRef<'s, $ref<'f, D>>> for TmpRef<'s, ItemRef<'f, D>> {
			fn from(TmpRef { inner, _marker }: TmpRef<'s, $ref<'f, D>>) -> Self {
				let inner = mem::ManuallyDrop::into_inner(inner);
				Self { inner: mem::ManuallyDrop::new(ItemRef::$var(inner)), _marker }
			}
		}

		impl<'f, D: Dev> From<$ref<'f, D>> for ItemRef<'f, D> {
			fn from(r: $ref<'f, D>) -> Self {
				Self::$var(r)
			}
		}
	};
}

impl_tmpref!(RawDirRef DirRef Dir);
impl_tmpref!(RawFileRef FileRef File);
impl_tmpref!(RawSymRef SymRef Sym);
impl_tmpref!(RawUnknownRef UnknownRef Unknown);

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
	#[cfg(test)]
	debug_assert_eq!(l, data.len());
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
	#[cfg(test)]
	debug_assert_eq!(l, buf.len());
	(l == buf.len()).then_some(()).ok_or(Error::Truncated)
}

/// Data header, shared by [`DirData`] and [`FileData`].
#[derive(Clone, Debug)]
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

/// Panic if a type is being dropped when it shouldn't be.
///
/// Used by [`FileRef`] et al.
///
/// # Note
///
/// Doesn't panic if it is called during another panic to avoid an abort.
fn forbid_drop() {
	if !std::thread::panicking() {
		panic!("drop is forbidden");
	}
}
