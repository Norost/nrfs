//#![cfg_attr(not(test), no_std)]
#![forbid(unused_must_use)]
#![forbid(elided_lifetimes_in_paths)]
#![feature(iterator_try_collect)]
#![feature(cell_update)]
#![feature(split_array)]
#![feature(error_in_core)]

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

extern crate alloc;

mod config;
pub mod dir;
mod file;
mod name;
#[cfg(any(test, fuzzing))]
pub mod test;

pub use {
	config::{LoadConfig, NewConfig},
	dir::DirOptions,
	name::Name,
	nros::{
		dev, BlockSize, CipherType, Compression, Dev, KeyDeriver, KeyPassword, MaxRecordSize,
		Resource,
	},
};

use {
	alloc::collections::BTreeMap,
	core::{
		cell::{RefCell, RefMut},
		fmt,
		future::Future,
		marker::PhantomData,
		mem,
		ops::{self, Deref, DerefMut, IndexMut},
	},
	dir::{DirData, Index, ItemRef, ObjectId, Offset},
	file::FileData,
};

/// Type used as index in [`File`].
type Idx = u64;

#[derive(Debug, Default)]
struct Files {
	/// File data.
	///
	/// A BTreeMap is used since it automatically shrinks itself (cheaply)
	/// and is DoS-resistant.
	data: BTreeMap<u64, FileData>,
	/// File index counter
	///
	/// This is used to generate new indices for indexing into [`files`]
	idx_counter: u64,
}

impl Files {
	pub fn get_mut(&mut self, idx: Idx) -> Option<&mut FileData> {
		self.data.get_mut(&idx)
	}

	pub fn remove(&mut self, idx: Idx) -> Option<FileData> {
		self.data.remove(&idx)
	}

	pub fn insert(&mut self, data: FileData) -> Idx {
		let idx = self.idx_counter;
		self.data.insert(idx, data);
		self.idx_counter += 1;
		idx
	}
}

impl ops::Index<Idx> for Files {
	type Output = FileData;

	fn index(&self, index: Idx) -> &Self::Output {
		&self.data[&index]
	}
}

impl IndexMut<Idx> for Files {
	fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
		self.get_mut(index).expect("no file data with index")
	}
}

/// [`Nrfs`] shared mutable data.
#[derive(Debug, Default)]
struct NrfsData {
	/// Files with live references.
	///
	/// Since filess may be embedded at any time using IDs directly is not practical.
	files: Files,
	/// Directories with live references.
	///
	/// Indexed by ID.
	directories: BTreeMap<ObjectId, DirData>,
}

/// NRFS filesystem manager.
#[derive(Debug)]
pub struct Nrfs<D: Dev> {
	/// Object storage.
	storage: nros::Nros<D, nros::StdResource>,
	/// Data of objects with live references.
	data: RefCell<NrfsData>,
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
		let mut s = Self { storage, data: Default::default(), read_only: false };
		DirRef::new_root(&mut s, &dir).await?.drop().await?;
		Ok(s)
	}

	/// `read_only` guarantees no modifications will be made.
	// TODO read_only is a sham.
	pub async fn load(config: LoadConfig<'_, D>) -> Result<Self, Error<D>> {
		let LoadConfig { devices, cache_size, allow_repair, retrieve_key } = config;
		let conf = nros::LoadConfig {
			devices,
			cache_size,
			allow_repair,
			retrieve_key,
			resource: nros::StdResource::new(),
			magic: Self::MAGIC,
		};
		Ok(Self {
			storage: nros::Nros::load(conf).await?,
			data: Default::default(),
			read_only: !allow_repair,
		})
	}

	/// Get a reference to the root directory.
	pub async fn root_dir(&self) -> Result<DirRef<'_, D>, Error<D>> {
		DirRef::load_root(self).await
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

	/// Get a reference to a [`FileData`] structure.
	fn file_data(&self, idx: Idx) -> RefMut<'_, FileData> {
		RefMut::map(self.data.borrow_mut(), |fs| &mut fs.files[idx])
	}

	/// Get a reference to a [`DirData`] structure.
	fn dir_data(&self, id: ObjectId) -> RefMut<'_, DirData> {
		RefMut::map(self.data.borrow_mut(), |fs| {
			fs.directories.get_mut(&id).expect("no DirData with id")
		})
	}

	/// Get an object.
	async fn get(&self, id: ObjectId) -> Result<nros::Object<'_, D, nros::StdResource>, Error<D>> {
		Ok(self.storage.get(id.into()).await?)
	}
}

/// Trait to convert between "raw" and "complete" references,
/// i.e. references without direct access to the filesystem
/// and references with.
pub trait RawRef<'a, D: Dev>: Sized {
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
	id: ObjectId,
}

/// Raw [`DirRef`] data.
///
/// This is more compact than [`DirRef`] and better suited for storing in a container.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use = "value must be used to avoid reference leaks"]
pub struct RawDirRef {
	/// ID of the directory object.
	id: ObjectId,
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

impl<'a, T> TmpRef<'a, T> {
	/// Erase the associated lifetime.
	fn erase_lifetime<'x>(self) -> TmpRef<'x, T> {
		TmpRef { inner: self.inner, _marker: PhantomData }
	}
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
	($raw:ident $ref:ident $var:ident $v:ident $v_deref:expr) => {
		impl $raw {
			/// Create a "temporary" reference.
			///
			/// See [`TmpRef`] for more information.
			pub fn into_tmp<'s, 'a, D: Dev>(&'s self, fs: &'a Nrfs<D>) -> TmpRef<'s, $ref<'a, D>> {
				TmpRef {
					inner: mem::ManuallyDrop::new($ref::from_raw(fs, self.clone())),
					_marker: PhantomData,
				}
			}
		}

		impl<'s, 'a, D: Dev> From<TmpRef<'s, $ref<'a, D>>> for TmpRef<'s, ItemRef<'a, D>> {
			fn from(TmpRef { inner, _marker }: TmpRef<'s, $ref<'a, D>>) -> Self {
				let inner = mem::ManuallyDrop::into_inner(inner);
				Self { inner: mem::ManuallyDrop::new(ItemRef::$var(inner)), _marker }
			}
		}

		impl<'s, 'a, D: Dev> From<&'s $ref<'a, D>> for TmpRef<'s, ItemRef<'a, D>> {
			fn from($v: &'s $ref<'a, D>) -> Self {
				$v_deref
					.as_raw()
					.into_tmp($v_deref.fs)
					.erase_lifetime()
					.into()
			}
		}

		impl<'a, D: Dev> From<$ref<'a, D>> for ItemRef<'a, D> {
			fn from($v: $ref<'a, D>) -> Self {
				Self::$var($v)
			}
		}
	};
}

impl_tmpref!(RawDirRef DirRef Dir ref_ ref_);
impl_tmpref!(RawFileRef FileRef File ref_ ref_);
impl_tmpref!(RawSymRef SymRef Sym ref_ ref_.0);
impl_tmpref!(RawUnknownRef UnknownRef Unknown ref_ ref_.0);

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

/// Read an exact amount of data.
///
/// Fails if the buffer could not be filled.
async fn read_exact<'a, D: Dev>(
	obj: &nros::Object<'a, D, nros::StdResource>,
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
	parent_id: ObjectId,
	/// Offset in the parent directory.
	///
	/// Not applicable if the ID of the object is 0,
	/// i.e. it is the root directory.
	parent_index: Index,
}

impl DataHeader {
	/// Create a new header.
	fn new(parent_id: ObjectId, parent_index: Index) -> Self {
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
	if cfg!(test) && !std::thread::panicking() {
		panic!("drop is forbidden");
	}
	eprintln!("drop is forbidden");
}
