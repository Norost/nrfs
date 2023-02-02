//#![cfg_attr(not(test), no_std)]
#![forbid(unused_must_use)]
#![forbid(elided_lifetimes_in_paths)]
#![feature(iterator_try_collect)]
#![feature(cell_update)]
#![feature(pin_macro)]
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
		dev, Background, BlockSize, CipherType, Compression, Dev, KeyDeriver, KeyPassword,
		MaxRecordSize, Resource,
	},
};

use {
	core::{
		cell::{RefCell, RefMut},
		fmt,
		marker::PhantomData,
		mem,
		ops::{Deref, DerefMut},
	},
	dir::{DirData, ItemRef},
	file::FileData,
	rustc_hash::FxHashMap,
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
		let bg = Background::default();
		DirRef::new_root(&bg, &mut s, &dir).await?.drop().await?;
		bg.drop().await?;
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
	pub async fn root_dir<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
	) -> Result<DirRef<'a, 'b, D>, Error<D>> {
		DirRef::load_root(bg, self).await
	}

	pub async fn finish_transaction<'a, 'b>(
		&'a self,
		bg: &'b Background<'a, D>,
	) -> Result<(), Error<D>> {
		self.storage
			.finish_transaction(bg)
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
	fn dir_data(&self, id: u64) -> RefMut<'_, DirData> {
		RefMut::map(self.data.borrow_mut(), |fs| {
			fs.directories.get_mut(&id).expect("no DirData with id")
		})
	}
}

/// Trait to convert between "raw" and "complete" references,
/// i.e. references without direct access to the filesystem
/// and references with.
pub trait RawRef<'a, 'b, D: Dev>: Sized {
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
	fn from_raw(fs: &'a Nrfs<D>, bg: &'b Background<'a, D>, raw: Self::Raw) -> Self;
}

/// Reference to a directory object.
#[derive(Debug)]
#[must_use = "Must be manually dropped with DirRef::drop"]
pub struct DirRef<'a, 'b, D: Dev> {
	/// Filesystem object containing the directory.
	fs: &'a Nrfs<D>,
	/// Background task runner.
	bg: &'b Background<'a, D>,
	/// ID of the directory object.
	id: u64,
}

/// Raw [`DirRef`] data.
///
/// This is more compact than [`DirRef`] and better suited for storing in a container.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use = "value must be used to avoid reference leaks"]
pub struct RawDirRef {
	/// ID of the directory object.
	id: u64,
}

impl<'a, 'b, D: Dev> RawRef<'a, 'b, D> for DirRef<'a, 'b, D> {
	type Raw = RawDirRef;

	fn as_raw(&self) -> RawDirRef {
		RawDirRef { id: self.id }
	}

	fn from_raw(fs: &'a Nrfs<D>, bg: &'b Background<'a, D>, raw: RawDirRef) -> Self {
		Self { fs, bg, id: raw.id }
	}
}

impl<'a, 'b, D: Dev> Clone for DirRef<'a, 'b, D> {
	fn clone(&self) -> Self {
		self.fs.dir_data(self.id).header.reference_count += 1;
		Self { fs: self.fs, bg: self.bg, id: self.id }
	}
}

impl<D: Dev> Drop for DirRef<'_, '_, D> {
	fn drop(&mut self) {
		forbid_drop()
	}
}

/// Reference to a file object.
#[derive(Debug)]
#[must_use = "Must be manually dropped with FileRef::drop"]
pub struct FileRef<'a, 'b, D: Dev> {
	/// Filesystem object containing the directory.
	fs: &'a Nrfs<D>,
	/// Background task runner.
	bg: &'b Background<'a, D>,
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

impl<'a, 'b, D: Dev> RawRef<'a, 'b, D> for FileRef<'a, 'b, D> {
	type Raw = RawFileRef;

	fn as_raw(&self) -> RawFileRef {
		RawFileRef { idx: self.idx }
	}

	fn from_raw(fs: &'a Nrfs<D>, bg: &'b Background<'a, D>, raw: RawFileRef) -> Self {
		Self { fs, bg, idx: raw.idx }
	}
}

impl<'a, 'b, D: Dev> Clone for FileRef<'a, 'b, D> {
	fn clone(&self) -> Self {
		self.fs.file_data(self.idx).header.reference_count += 1;
		Self { fs: self.fs, bg: self.bg, idx: self.idx }
	}
}

impl<D: Dev> Drop for FileRef<'_, '_, D> {
	fn drop(&mut self) {
		forbid_drop()
	}
}

/// Reference to a file object representing a symbolic link.
#[derive(Clone, Debug)]
#[must_use = "Must be manually dropped with SymRef::drop"]
pub struct SymRef<'a, 'b, D: Dev>(FileRef<'a, 'b, D>);

/// Raw [`SymRef`] data.
///
/// This is more compact than [`SymRef`] and better suited for storing in a container.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use = "value must be used to avoid reference leaks"]
pub struct RawSymRef(RawFileRef);

impl<'a, 'b, D: Dev> RawRef<'a, 'b, D> for SymRef<'a, 'b, D> {
	type Raw = RawSymRef;

	fn as_raw(&self) -> RawSymRef {
		RawSymRef(self.0.as_raw())
	}

	fn from_raw(fs: &'a Nrfs<D>, bg: &'b Background<'a, D>, raw: RawSymRef) -> Self {
		SymRef(FileRef::from_raw(fs, bg, raw.0))
	}
}

/// Reference to an entry with an unrecognized type.
#[derive(Clone, Debug)]
#[must_use = "Must be manually dropped with UnknownRef::drop"]
pub struct UnknownRef<'a, 'b, D: Dev>(FileRef<'a, 'b, D>);

/// Raw [`UnknownRef`] data.
///
/// This is more compact than [`UnknownRef`] and better suited for storing in a container.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use = "value must be used to avoid reference leaks"]
pub struct RawUnknownRef(RawFileRef);

impl<'a, 'b, D: Dev> RawRef<'a, 'b, D> for UnknownRef<'a, 'b, D> {
	type Raw = RawUnknownRef;

	fn as_raw(&self) -> RawUnknownRef {
		RawUnknownRef(self.0.as_raw())
	}

	fn from_raw(fs: &'a Nrfs<D>, bg: &'b Background<'a, D>, raw: RawUnknownRef) -> Self {
		UnknownRef(FileRef::from_raw(fs, bg, raw.0))
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
			pub fn into_tmp<'s, 'a, 'b, D: Dev>(
				&'s self,
				fs: &'a Nrfs<D>,
				bg: &'b Background<'a, D>,
			) -> TmpRef<'s, $ref<'a, 'b, D>> {
				TmpRef {
					inner: mem::ManuallyDrop::new($ref::from_raw(fs, bg, self.clone())),
					_marker: PhantomData,
				}
			}
		}

		impl<'s, 'a, 'b, D: Dev> From<TmpRef<'s, $ref<'a, 'b, D>>>
			for TmpRef<'s, ItemRef<'a, 'b, D>>
		{
			fn from(TmpRef { inner, _marker }: TmpRef<'s, $ref<'a, 'b, D>>) -> Self {
				let inner = mem::ManuallyDrop::into_inner(inner);
				Self { inner: mem::ManuallyDrop::new(ItemRef::$var(inner)), _marker }
			}
		}

		impl<'a, 'b, D: Dev> From<$ref<'a, 'b, D>> for ItemRef<'a, 'b, D> {
			fn from(r: $ref<'a, 'b, D>) -> Self {
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

/// Write an exact amount of data.
///
/// Fails if not all data could be written.
async fn write_all<'a, 'b, D: Dev>(
	obj: &nros::Tree<'a, 'b, D, nros::StdResource>,
	offset: u64,
	data: &[u8],
) -> Result<(), Error<D>> {
	let l = obj.write(offset, data).await?;
	#[cfg(test)]
	debug_assert_eq!(l, data.len());
	(l == data.len()).then_some(()).ok_or(Error::Truncated)
}

/// Write an exact amount of data.
///
/// This function automatically grows the object if it can't contain the data.
async fn write_grow<'a, 'b, D: Dev>(
	obj: &nros::Tree<'a, 'b, D, nros::StdResource>,
	offset: u64,
	data: &[u8],
) -> Result<(), Error<D>> {
	if obj.len().await? < offset + data.len() as u64 {
		obj.resize(offset + data.len() as u64).await?;
	}
	write_all(&obj, offset, data).await
}

/// Read an exact amount of data.
///
/// Fails if the buffer could not be filled.
async fn read_exact<'a, 'b, D: Dev>(
	obj: &nros::Tree<'a, 'b, D, nros::StdResource>,
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
	if cfg!(test) && !std::thread::panicking() {
		panic!("drop is forbidden");
	}
	eprintln!("drop is forbidden");
}
