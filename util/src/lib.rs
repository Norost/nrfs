#![no_std]
#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]
#![feature(cell_update)]

extern crate alloc;

pub mod task;

use {
	alloc::{boxed::Box, collections::btree_map},
	core::{future::Future, pin::Pin},
};

/// Box a future and erase its type.
pub fn box_fut<'a, T: Future + 'a>(fut: T) -> Pin<Box<dyn Future<Output = T::Output> + 'a>> {
	Box::pin(fut)
}

/// Extra methods for [`btree_map::BTreeMap`]
pub trait BTreeMapExt<K, V> {
	/// Get a [`btree_map::OccupiedEntry`].
	fn occupied(&mut self, key: K) -> Option<btree_map::OccupiedEntry<'_, K, V>>;
}

impl<K: Ord + Eq, V> BTreeMapExt<K, V> for btree_map::BTreeMap<K, V> {
	fn occupied(&mut self, key: K) -> Option<btree_map::OccupiedEntry<'_, K, V>> {
		let btree_map::Entry::Occupied(e) = self.entry(key) else { return None };
		Some(e)
	}
}
