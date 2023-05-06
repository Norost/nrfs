use {
	crate::{apply_u48, u48_to_u64, u64_to_u48, Tag, HAMT_ENTRY_SIZE, HAMT_ROOT_LEN, HEADER_SIZE},
	core::fmt,
};

#[derive(Default)]
#[repr(C)]
struct FreeRegion {
	offset: [u8; 6],
	length: [u8; 6],
}

#[repr(C)]
pub(crate) struct Header {
	pub hash_key: [u8; 16],
	used: [u8; 6],
	free_head: [u8; 6],
	free_regions: [FreeRegion; 3],
}

impl Header {
	pub fn new(hash_key: [u8; 16], offset: u64) -> Self {
		let used = offset + HEADER_SIZE + HAMT_ENTRY_SIZE * HAMT_ROOT_LEN;
		let used = (used + 15) & !15;
		Self {
			hash_key,
			used: u64_to_u48(used).unwrap(),
			free_head: u64_to_u48(used + 8).unwrap(),
			free_regions: Default::default(),
		}
	}

	pub fn to_raw(&self) -> [u8; HEADER_SIZE as _] {
		fn f<const N: usize>(s: &mut [u8], v: [u8; N]) -> &mut [u8] {
			let (x, y) = s.split_array_mut::<N>();
			*x = v;
			y
		}
		let fr = |b, r: &FreeRegion| {
			let b = f(b, r.offset);
			f(b, r.length)
		};

		let mut buf = [0; _];
		let b = f(&mut buf, self.hash_key);
		let b = f(b, self.used);
		let b = f(b, self.free_head);
		let b = fr(b, &self.free_regions[0]);
		let b = fr(b, &self.free_regions[1]);
		let b = fr(b, &self.free_regions[2]);
		assert!(b.is_empty());
		buf
	}

	pub fn from_raw(raw: &[u8; HEADER_SIZE as _]) -> Self {
		fn f<const N: usize>(s: &mut &[u8]) -> [u8; N] {
			let (x, y) = s.split_array_ref::<N>();
			*s = y;
			*x
		}
		let fr = |s: &mut &[u8]| FreeRegion { offset: f(s), length: f(s) };

		let raw = &mut &raw[..];
		let s = Self {
			hash_key: f(raw),
			used: f(raw),
			free_head: f(raw),
			free_regions: [fr(raw), fr(raw), fr(raw)],
		};
		assert!(raw.is_empty());
		s
	}

	pub fn alloc(&mut self, amount: u64) -> Option<(Tag, u64)> {
		for r in &mut self.free_regions {
			let l = u48_to_u64(r.length);
			if l >= amount {
				let offt = Tag::new(u48_to_u64(r.offset))?;
				r.offset = apply_u48(r.offset, |n| n + amount)?;
				r.length = apply_u48(r.length, |n| n - amount)?;
				self.used = apply_u48(self.used, |n| n + amount).unwrap();
				return Some((offt, l));
			}
		}
		let offt = Tag::new(u48_to_u64(self.free_head))?;
		self.free_head = apply_u48(self.free_head, |n| n + amount)?;
		self.used = apply_u48(self.used, |n| n + amount).unwrap();
		Some((offt, 0))
	}

	pub fn dealloc(&mut self, amount: u64) -> Option<()> {
		self.used = apply_u48(self.used, |n| n - amount)?;
		Some(())
	}

	pub fn insert_free_region(&mut self, mut offset: u64, length: u64) -> bool {
		// Merge
		let mut end = offset + length;
		for r in &mut self.free_regions {
			let o = u48_to_u64(r.offset);
			let e = o + u48_to_u64(r.length);
			// |aaaa|baba|bbbb|
			// o  offset e
			if (o..=e).contains(&offset) {
				[r.offset, r.length] = [[0; 6]; 2];
				offset = offset.min(o);
			}
			//   |aaaa|baba|bbbb|
			// offset o   end
			if (offset..=end).contains(&o) {
				[r.offset, r.length] = [[0; 6]; 2];
				end = end.max(e);
			}
		}

		// If at or past head, shrink
		if u48_to_u64(self.free_head) <= end {
			self.free_head = u64_to_u48(offset).unwrap();
			return true;
		}

		// Insert
		let mut min_i = 0;
		let mut min_l = u64::MAX;
		for (i, r) in self.free_regions.iter_mut().enumerate() {
			let l = u48_to_u64(r.length);
			if min_l > l {
				min_i = i;
				min_l = l;
			}
		}
		let length = u64_to_u48(end - offset).unwrap();
		let offset = u64_to_u48(offset).unwrap();
		self.free_regions[min_i] = FreeRegion { offset, length };

		// Sort by offset
		let mut f = |x: usize, y: usize| {
			if u48_to_u64(self.free_regions[x].offset) > u48_to_u64(self.free_regions[y].offset) {
				self.free_regions.swap(x, y);
			}
		};
		// Bubble sort
		f(0, 1);
		f(1, 2);
		f(0, 1);
		false
	}
}

impl fmt::Debug for FreeRegion {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		format_args!("{}+{}", u48_to_u64(self.offset), u48_to_u64(self.length)).fmt(f)
	}
}

impl fmt::Debug for Header {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct(stringify!(Header))
			.field("hash_key", &u128::from_le_bytes(self.hash_key))
			.field("used", &u48_to_u64(self.used))
			.field("free_head", &u48_to_u64(self.free_head))
			.field("free_regions", &self.free_regions)
			.finish()
	}
}
