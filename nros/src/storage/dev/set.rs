use {
	super::{Allocator, Buf, Dev},
	crate::{
		data::{
			cipher::Cipher,
			fs_info::{Configuration, FsHeader, FsInfo, MirrorCount, MirrorIndex},
			record::{Depth, RecordRef},
		},
		key_derivation, BlockSize, CipherType, Compression, Error, KeyDerivation, KeyDeriver,
		KeyPassword, LoadConfig, MaxRecordSize, NewConfig, Resource,
	},
	alloc::sync::Arc,
	core::{cell::Cell, fmt, future, mem},
	futures_util::stream::{FuturesOrdered, FuturesUnordered, StreamExt, TryStreamExt},
};

/// A single device with some extra information.
#[derive(Debug)]
struct Node<D> {
	/// The device itself.
	dev: D,
	/// The offset of the blocks of this device in the chain.
	block_offset: u64,
	/// The amount of blocks covered by this device.
	block_count: u64,
}

/// Wrapper around a set of devices.
pub(crate) struct DevSet<D: Dev, R: Resource> {
	/// The devices and their respective block counts.
	///
	/// It is an array of mirrors, which in turns represents a chain of devices.
	devices: Box<[Box<[Node<D>]>]>,
	/// The size of a block.
	block_size: BlockSize,
	/// The maximum size of a single record.
	max_record_size: MaxRecordSize,
	/// The default compression to use for records.
	compression: Compression,
	/// The total amount of blocks covered in each chain.
	block_count: u64,
	/// The unique identifier of this filesystem.
	uid: [u8; 16],

	pub allocation_log_head: Cell<RecordRef>,

	pub object_list_root: Cell<RecordRef>,
	pub object_bitmap_root: Cell<RecordRef>,
	pub object_list_depth: Cell<Depth>,

	/// Resources for allocation & parallel processing.
	pub resource: Arc<R>,

	/// Magic value to copy to the header.
	magic: [u8; 4],

	/// Key to encrypt the header with.
	header_key: Cell<[u8; 32]>,
	key_derivation: Cell<KeyDerivation>,
	key_hash: [u8; 2],

	cipher: CipherType,
	nonce: Cell<u64>,

	key: [u8; 32],
}

impl<D: Dev, R: Resource> DevSet<D, R> {
	/// Create a new device set.
	///
	/// # Note
	///
	/// This writes new headers to the devices.
	///
	/// # Panics
	///
	/// If `mirror` is empty.
	///
	/// If any chain is empty.
	///
	/// If any device has no blocks.
	pub async fn new(config: NewConfig<'_, D, R>) -> Result<Self, Error<D>> {
		// Determine length of smallest chain.
		let block_count = config
			.mirrors
			.iter()
			.map(|c| c.iter().map(|d| d.block_count() - 2).sum::<u64>())
			.min()
			.expect("no chains");

		// Collect devices into a convenient format.
		let mut devices = config
			.mirrors
			.into_iter()
			.map(|chain| {
				// Don't exceed the block count of the smallest chain.
				let mut remaining_blocks = block_count;
				chain
					.into_iter()
					.map(|dev| {
						let block_count = remaining_blocks.min(dev.block_count() - 2);
						remaining_blocks -= block_count;
						Node { block_count, dev, block_offset: 0 }
					})
					.collect::<Box<_>>()
			})
			.collect::<Box<_>>();

		let block_size = config.block_size.to_raw();
		let max_record_size = config.max_record_size.to_raw();

		assert!(devices.iter().all(|c| !c.is_empty()), "empty chain");
		// Require that devices can contain a full-sized record to simplify
		// read & write operations as well as ensure some sanity in general.
		assert!(
			devices
				.iter()
				.flat_map(|c| c.iter())
				.all(|d| d.dev.block_count() >= 2 + (1 << (max_record_size - block_size))),
			"device cannot contain maximum size record & headers"
		);

		// FIXME support mismatched block sizes.
		assert!(
			devices
				.iter()
				.flat_map(|c| c.iter())
				.all(|d| d.dev.block_size().to_raw() == block_size),
			"todo: support mismatched block sizes"
		);

		// Assign block offsets to devices in chains and write headers.
		for chain in devices.iter_mut() {
			let mut block_offset = 0;
			for node in chain.iter_mut() {
				node.block_offset = block_offset;
				block_offset += node.block_count;
			}
		}

		// Generate key & UID.
		let mut buf = [0; 48];
		config.resource.crng_fill(&mut buf);
		let (mut key, mut uid) = ([0; 32], [0; 16]);
		key.copy_from_slice(&buf[..32]);
		uid.copy_from_slice(&buf[32..]);

		// Get or derive key for header.
		let (header_key, key_derivation) = match config.key_deriver {
			KeyDeriver::None { key } => (*key, KeyDerivation::None),
			KeyDeriver::Argon2id { password, m, t, p } => {
				let kdf = KeyDerivation::Argon2id { m, t, p };
				(key_derivation::argon2id(password, &uid, m, t, p), kdf)
			}
		};

		let key_hash = FsHeader::hash_key(&header_key);

		Ok(Self {
			devices,
			block_size: config.block_size,
			max_record_size: config.max_record_size,
			compression: config.compression,
			block_count,

			magic: config.magic,
			uid,

			allocation_log_head: Default::default(),

			object_list_root: Default::default(),
			object_bitmap_root: Default::default(),
			object_list_depth: Depth::D0.into(),

			cipher: config.cipher,

			header_key: header_key.into(),
			key_derivation: key_derivation.into(),
			key_hash,

			key,
			nonce: Default::default(),

			resource: config.resource.into(),
		})
	}

	/// Load an existing device set.
	///
	/// `read_only` prevents fixing any headers that may be broken.
	pub async fn load(config: LoadConfig<'_, D, R>) -> Result<Self, Error<D>> {
		// We're looking to retrieve two types of data from the devices:
		//
		// 1. Global info that is shared among all devices.
		// 2. Per-device info that we need to retrieve per device.
		//
		// For global info any valid header from any device will do.
		// For per-device info we need any valid header per device.

		let mut header_key = None;

		// Collect both start & end headers.
		let headers = config
			.devices
			.iter()
			.flat_map(|d| {
				let end = d.block_count() - 1;
				let block_size = 1 << d.block_size().to_raw();
				[d.read(0, block_size), d.read(end, block_size)]
			})
			.collect::<FuturesOrdered<_>>()
			.map(|buf| {
				// Try to decrypt header
				let Ok(mut buf) = buf else { return None };

				let (hdr, info) = buf.get_mut().split_at_mut(64);
				let header = FsHeader::from_raw((&*hdr).try_into().unwrap());

				let key = match header_key.as_ref() {
					Some(h) => *h,
					None if matches!(header.cipher(), Ok(CipherType::NoneXxh3)) => [0; 32],
					None => match header.key_derivation().unwrap() {
						KeyDerivation::None => match (config.retrieve_key)(false).unwrap() {
							KeyPassword::Key(k) => k,
							KeyPassword::Password(_) => panic!("expected key"),
						},
						KeyDerivation::Argon2id { p, t, m } => {
							match (config.retrieve_key)(true).unwrap() {
								KeyPassword::Key(k) => k,
								KeyPassword::Password(pwd) => loop {
									let key = key_derivation::argon2id(&pwd, &header.uid, m, t, p);
									if header.verify_key(&key) {
										break key;
									}
								},
							}
						}
					},
				};
				assert!(header.verify_key(&key), "key doesn't match");
				header_key = Some(key);
				header.decrypt(&key, info).ok()?;
				Some(buf)
			})
			.collect::<Vec<_>>()
			.await;

		let has_broken_headers = headers.iter().any(|b| b.is_none());
		let mut header = None;

		// FIXME check UIDs

		// Build mirrors
		let mut mirrors = vec![vec![]; 4];
		for (i, bufs) in headers.chunks(2).enumerate() {
			let [buf_a, buf_b] = bufs else { unreachable!() };
			let buf = buf_a.as_ref().or(buf_b.as_ref()).expect("no valid header");

			let (hdr, info) = buf.get().split_at(64);

			let hdr = FsHeader::from_raw(hdr.try_into().unwrap());
			let (info, _) = FsInfo::from_raw_slice(info).unwrap();

			// Add to mirror.
			mirrors
				.get_mut(usize::from(info.configuration.mirror_index().to_raw()))
				.expect("todo: invalid mirror index")
				.push((i, u64::from(info.lba_offset), u64::from(info.block_count)));

			header.get_or_insert((hdr, info));
		}
		let (header, info) = header.expect("no header");
		let header_key = header_key.expect("no header key");

		drop(headers);

		assert_eq!(header.version, FsHeader::VERSION, "header version mismatch");

		let mirc = info.configuration.mirror_count().to_raw();
		let rem_empty = mirrors.drain(usize::from(mirc)..).all(|c| c.is_empty());
		assert!(rem_empty);

		// Sort each device and check for gaps.
		for chain in mirrors.iter_mut() {
			chain.sort_unstable_by_key(|(_, lba_offset, _)| *lba_offset);
			// TODO check gaps
			let mut next_lba = 0;
			for &(_, lba_offset, block_count) in chain.iter() {
				// FIXME don't panic, return error instead.
				assert_eq!(lba_offset, next_lba, "gap in chain");
				next_lba += block_count;
			}
			assert_eq!(next_lba, info.total_block_count, "gap in chain");
		}

		// TODO avoid conversion to Vec<Option<_>>
		let mut devices = config
			.devices
			.into_iter()
			.map(|d| Some(d))
			.collect::<Vec<_>>();

		// Collect in order.
		let devices = mirrors
			.into_iter()
			.map(|chain| {
				chain
					.into_iter()
					.map(|(i, block_offset, block_count)| Node {
						dev: devices[i].take().unwrap(),
						block_offset,
						block_count,
					})
					.collect()
			})
			.collect();

		let hc = &info.configuration;

		let s = Self {
			devices,

			block_size: header.block_size(),
			max_record_size: hc.max_record_size(),
			compression: hc.compression_algorithm().unwrap(),
			uid: header.uid,
			block_count: info.total_block_count.into(),

			object_list_root: info.object_list_root.into(),
			object_bitmap_root: info.object_bitmap_root.into(),
			allocation_log_head: info.allocation_log_head.into(),

			cipher: header.cipher().unwrap(),
			header_key: header_key.into(),
			key_hash: FsHeader::hash_key(&header_key),

			key: info.key,
			key_derivation: header.key_derivation().unwrap().into(),

			object_list_depth: hc.object_list_depth().into(),

			magic: header.magic,
			nonce: Cell::new(header.nonce.into()),

			resource: config.resource.into(),
		};

		// If any headers are broken, fix them now.
		if config.allow_repair && has_broken_headers {
			s.save_headers().await?;
		}

		Ok(s)
	}

	/// Save headers to the start and end of all devices.
	///
	/// This performs a fence before writing to the start
	/// and another fence before writing to the end.
	pub async fn save_headers(&self) -> Result<(), Error<D>> {
		// First ensure all data is flushed.
		self.devices
			.iter()
			.flat_map(|chain| chain.iter())
			.map(|node| node.dev.fence())
			.collect::<FuturesUnordered<_>>()
			.try_for_each(|()| future::ready(Ok(())))
			.await
			.map_err(Error::Dev)?;

		/// Save a single header to a device.
		async fn save_header<D: Dev>(
			tail: bool,
			node: &Node<D>,
			header: <D::Allocator as Allocator>::Buf,
		) -> Result<(), D::Error> {
			let lba = if tail { 0 } else { 1 + node.block_count };
			node.dev.write(lba, header);
			node.dev.fence().await
		}

		// Allocate buffers for headers & write to start headers.
		let fut = self
			.devices
			.iter()
			.enumerate()
			.map(|(i, chain)| {
				let i = index_to_miri(i);
				chain.iter().map(move |node| async move {
					let buf = self.create_header(i, node).await?;
					let b = buf.clone();
					save_header(false, node, b).await?;
					Ok((node, buf))
				})
			})
			.flatten()
			.collect::<FuturesUnordered<_>>();

		// Wait for futures to finish.
		let fut = fut.try_collect::<Vec<_>>().await.map_err(Error::Dev)?;

		// Now save to end.
		let fut: FuturesUnordered<_> = fut
			.into_iter()
			.map(|(node, buf)| save_header(true, node, buf))
			.collect::<FuturesUnordered<_>>();

		// Wait for futures to finish
		fut.try_for_each(|f| future::ready(Ok(f)))
			.await
			.map_err(Error::Dev)
	}

	/// Read a range of blocks.
	///
	/// A chain blacklist can be used in case corrupt data was returned.
	///
	/// The chain from which the data is read is returned.
	///
	/// If all available chains are blacklisted, `None` is returned.
	///
	/// # Note
	///
	/// If `size` isn't a multiple of the block size.
	pub async fn read(
		&self,
		lba: u64,
		size: usize,
		blacklist: &Set256,
	) -> Result<Option<(SetBuf<D>, u8)>, (Error<D>, u8)> {
		assert!(
			size % (1 << self.block_size().to_raw()) == 0,
			"data len isn't a multiple of block size"
		);

		let lba_end =
			lba.saturating_add(u64::try_from(size >> self.block_size().to_raw()).unwrap());
		assert!(lba_end <= self.block_count, "read is out of bounds");

		// TODO balance loads
		for (i, chain) in self.devices.iter().enumerate() {
			let i = u8::try_from(i).unwrap();
			if blacklist.get(i) {
				continue;
			}

			// Do a binary search for the start device.
			let node_i = chain
				.binary_search_by_key(&lba, |node| node.block_offset)
				// if offset == lba, then we need that dev
				// if offset < lba, then we want the previous dev.
				.map_or_else(|i| i - 1, |i| i);
			let node = &chain[node_i];

			let node_block_end = node.block_offset + node.block_count;
			let node_lba = lba - node.block_offset + 1;

			// Check if the buffer range falls entirely within the device's range.
			// If not, split the buffer in two and perform two operations.
			return if lba_end <= node_block_end {
				// No splitting necessary - yay
				node.dev
					.read(node_lba, size)
					.await
					.map(|buf| Some((SetBuf(buf), i)))
					.map_err(|e| (Error::Dev(e), i))
			} else {
				// We need to split - aw
				// Figure out midpoint to split.
				let mid = size
					- usize::try_from(lba_end - node_block_end << self.block_size().to_raw())
						.unwrap();
				// Allocate two buffers and read into each.
				let (buf_l, buf_r, mut buf) = futures_util::try_join!(
					node.dev.read(node_lba, mid),
					chain[node_i + 1].dev.read(1, size - mid),
					node.dev.allocator().alloc(size),
				)
				.map_err(|e| (Error::Dev(e), i))?;
				// Merge buffers.
				let b = buf.get_mut();
				b[..mid].copy_from_slice(buf_l.get());
				b[mid..].copy_from_slice(buf_r.get());
				Ok(Some((SetBuf(buf), i)))
			};
		}

		Ok(None)
	}

	/// Write a range of blocks.
	///
	/// The whitelist indicates which chains to write to.
	///
	/// # Panics
	///
	/// If the buffer size isn't a multiple of the block size.
	///
	/// If the buffer overlaps more than two devices.
	///
	/// If the write is be out of bounds.
	pub async fn write(
		&self,
		lba: u64,
		data: SetBuf<D>,
		whitelist: Set256,
	) -> Result<(), Error<D>> {
		assert!(
			data.get().len() % (1 << self.block_size().to_raw()) == 0,
			"data len isn't a multiple of block size"
		);

		let lba_end = lba
			.saturating_add(u64::try_from(data.get().len() >> self.block_size().to_raw()).unwrap());
		assert!(lba_end <= self.block_count, "write is out of bounds");

		// Write to all mirrors
		self.devices
			.iter()
			.enumerate()
			.filter_map(|(i, chain)| whitelist.get(i.try_into().unwrap()).then(|| chain))
			.map(|chain| {
				// Do a binary search for the start device.
				let node_i = chain
					.binary_search_by_key(&lba, |node| node.block_offset)
					// if offset == lba, then we need that dev
					// if offset < lba, then we want the previous dev.
					.map_or_else(|i| i - 1, |i| i);
				let node = &chain[node_i];

				// Check if the buffer range falls entirely within the device's range.
				// If not, split the buffer in two and perform two operations.
				let data = &data;
				async move {
					let node_lba_end = node.block_offset + node.block_count;
					let node_lba = lba - node.block_offset + 1;
					if lba_end <= node_lba_end {
						// No splitting necessary - yay
						node.dev.write(node_lba, data.0.clone()).await // +1 because headers
					} else {
						// We need to split - aw
						let d = data.get();
						// Figure out midpoint to split.
						let mid = d.len()
							- usize::try_from(lba_end - node_lba_end << self.block_size().to_raw())
								.unwrap();
						// Allocate two buffers, copy halves to each and perform two writes.
						async fn f<D: Dev>(
							node: &Node<D>,
							lba: u64,
							data: &[u8],
						) -> Result<(), D::Error> {
							let mut buf = node.dev.allocator().alloc(data.len()).await?;
							buf.get_mut().copy_from_slice(data);
							node.dev.write(lba, buf).await
						}
						futures_util::try_join!(
							f(node, node_lba, &d[..mid]),
							f(&chain[node_i + 1], 1, &d[mid..])
						)
						.map(|((), ())| ())
					}
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_for_each(|_| async { Ok(()) })
			.await
			.map_err(Error::Dev)
	}

	/// Create a header for writing to a device.
	async fn create_header(
		&self,
		chain: MirrorIndex,
		node: &Node<D>,
	) -> Result<<D::Allocator as Allocator>::Buf, D::Error> {
		let mut buf = node
			.dev
			.allocator()
			.alloc(1 << self.block_size.to_raw())
			.await?;

		let mirc = u8::try_from(self.devices.len())
			.ok()
			.and_then(MirrorCount::from_raw)
			.expect("too many mirrors");

		let (header_raw, info_raw) = buf.get_mut().split_at_mut(64);

		let mut conf = Configuration::default();
		conf.set_mirror_count(mirc);
		conf.set_mirror_index(chain);
		conf.set_max_record_size(self.max_record_size());
		conf.set_object_list_depth(self.object_list_depth.get());
		conf.set_compression_level(0);
		conf.set_compression_algorithm(self.compression());

		let info = FsInfo {
			configuration: conf,
			lba_offset: node.block_offset.into(),
			block_count: node.block_count.into(),
			total_block_count: self.block_count.into(),
			key: self.key,

			object_list_root: self.object_list_root.get(),
			object_bitmap_root: self.object_bitmap_root.get(),
			allocation_log_head: self.allocation_log_head.get(),
		};
		info_raw[..mem::size_of::<FsInfo>()].copy_from_slice(info.as_ref());

		let (kdf, kdf_parameters) = self.key_derivation.get().to_raw();
		let mut header = FsHeader {
			magic: self.magic,
			version: FsHeader::VERSION,
			cipher: self.cipher.to_raw(),
			block_size: self.block_size.to_raw(),
			kdf,
			kdf_parameters,
			key_hash: self.key_hash,
			_reserved: [0; 6],
			uid: self.uid,
			nonce: self.nonce.get().into(),
			hash: [0; 16],
		};
		header.encrypt(&self.header_key(), info_raw);
		header_raw.copy_from_slice(header.as_ref());

		self.nonce.set(header.nonce.into());

		Ok(buf)
	}

	/// Allocate memory for writing.
	pub async fn alloc(&self, size: usize) -> Result<SetBuf<D>, Error<D>> {
		self.devices[0][0]
			.dev
			.allocator()
			.alloc(size)
			.await
			.map(SetBuf)
			.map_err(Error::Dev)
	}

	/// Return the underlying devices.
	pub fn into_devices(self) -> Vec<D> {
		self.devices
			.into_vec()
			.into_iter()
			.flat_map(|c| c.into_vec())
			.map(|n| n.dev)
			.collect()
	}

	/// The block size used by this device set.
	///
	/// This may differ from the underlying devices.
	/// The device set will automatically compensate for this.
	pub fn block_size(&self) -> BlockSize {
		self.block_size
	}

	/// The maximum size of a single record.
	pub fn max_record_size(&self) -> MaxRecordSize {
		self.max_record_size
	}

	/// The default compression to use for records.
	pub fn compression(&self) -> Compression {
		self.compression
	}

	/// The total amount of blocks addressable by this device set.
	pub fn block_count(&self) -> u64 {
		self.block_count
	}

	/// Get a cipher instance.
	pub fn cipher(&self) -> Cipher {
		Cipher { key: self.key, ty: self.cipher }
	}

	/// Generate a unique nonce.
	pub fn gen_nonce(&self) -> u64 {
		self.nonce.update(|x| x + 1)
	}

	/// Get the key used to encrypt the header.
	pub fn header_key(&self) -> [u8; 32] {
		self.header_key.get()
	}

	/// Set a new key derivation function.
	///
	/// This replaces the header key.
	pub fn set_key_deriver(&self, kdf: KeyDeriver<'_>) {
		let (key, kdf) = match kdf {
			KeyDeriver::None { key } => (*key, KeyDerivation::None),
			KeyDeriver::Argon2id { password, m, t, p } => {
				let key = key_derivation::argon2id(password, &self.uid, m, t, p);
				(key, KeyDerivation::Argon2id { m, t, p })
			}
		};
		self.header_key.set(key);
		self.key_derivation.set(kdf);
	}
}

/// Buffer for use with [`DevSet`].
pub struct SetBuf<D: Dev>(<D::Allocator as Allocator>::Buf);

impl<D: Dev> SetBuf<D> {
	/// Get an immutable reference to the data.
	pub fn get(&self) -> &[u8] {
		self.0.get()
	}

	/// Get a mutable reference to the data.
	pub fn get_mut(&mut self) -> &mut [u8] {
		self.0.get_mut()
	}

	/// Deallocate blocks at the tail until the desired size is reached.
	///
	/// # Panics
	///
	/// If the new size is larger than the current size.
	pub fn shrink(&mut self, size: usize) {
		self.0.shrink(size);
	}
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Set256(u128, u128);

impl Set256 {
	/// Create a new set with all bits set.
	pub fn set_all() -> Self {
		Self(u128::MAX, u128::MAX)
	}

	pub fn get(&self, bit: u8) -> bool {
		if bit < 0x80 {
			self.0 & 1 << bit > 0
		} else {
			let bit = bit & 0x7f;
			self.1 & 1 << bit > 0
		}
	}

	pub fn set(&mut self, bit: u8, value: bool) {
		if bit < 0x80 {
			self.0 &= !(1 << bit);
			self.0 |= u128::from(value) << bit;
		} else {
			let bit = bit & 0x7f;
			self.1 &= !(1 << bit);
			self.1 |= u128::from(value) << bit;
		}
	}
}

fn index_to_miri(i: usize) -> MirrorIndex {
	u8::try_from(i)
		.ok()
		.and_then(MirrorIndex::from_raw)
		.expect("mirror index out of range")
}

impl<D: Dev, R: Resource> fmt::Debug for DevSet<D, R> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(DevSet)).finish_non_exhaustive()
	}
}
