use {
	crate::{Error, Nrfs, Store},
	nrkv::{Key, Nrkv, StaticConf},
	nros::{Dev, Resource},
};

type AttrId = nrkv::Tag;

pub(crate) struct AttrMap<'a, D: Dev>(Nrkv<Store<'a, D>, StaticConf<0, 8>>);

impl<'a, D: Dev> AttrMap<'a, D> {
	pub async fn init(fs: &'a Nrfs<D>) -> Result<u64, Error<D>> {
		let id = fs.storage.create().await?.id();
		let key = &mut [0; 16];
		fs.resource().crng_fill(key);
		Self(Nrkv::init_with_key(Store { fs, id }, StaticConf, *key).await?);
		Ok(id)
	}

	pub async fn get_attr(&mut self, key: &Key) -> Result<Option<AttrId>, Error<D>> {
		let _lock = self.0.inner().0.fs.attr_map_lock.lock_inclusive().await;
		self.0.find(key).await
	}

	pub async fn ref_attr(&mut self, key: &Key) -> Result<AttrId, Error<D>> {
		let _lock = self.0.inner().0.fs.attr_map_lock.lock_exclusive().await;
		Ok(match self.0.insert(key, &1u64.to_le_bytes()).await? {
			Ok(id) => id,
			Err(id) => {
				let c = &mut [0; 8];
				self.0.read_user_data(id, 0, c).await?;
				let c = &(u64::from_le_bytes(*c).saturating_add(1)).to_le_bytes();
				self.0.write_user_data(id, 0, c).await?;
				id
			}
		})
	}

	pub async fn unref_attr(&mut self, id: AttrId) -> Result<(), Error<D>> {
		let _lock = self.0.inner().0.fs.attr_map_lock.lock_exclusive().await;
		let c = &mut [0; 8];
		self.0.read_user_data(id, 0, c).await?;
		let c = u64::from_le_bytes(*c) - 1;
		if c == u64::MAX - 1 {
			return Ok(());
		}
		if c > 0 {
			self.0.write_user_data(id, 0, &c.to_le_bytes()).await
		} else {
			self.0.remove(id).await
		}
	}

	pub async fn key(&mut self, id: AttrId) -> Result<Box<Key>, Error<D>> {
		let _lock = self.0.inner().0.fs.attr_map_lock.lock_inclusive().await;
		let len = self.0.read_key(id, &mut []).await?;
		let mut buf = vec![0; len.into()];
		self.0.read_key(id, &mut buf).await?;
		Ok(buf.into_boxed_slice().try_into().unwrap())
	}
}

impl<D: Dev> Nrfs<D> {
	pub(crate) async fn attr_map(&self) -> Result<AttrMap<'_, D>, Error<D>> {
		let id = u64::from_le_bytes(self.storage.header_data()[24..32].try_into().unwrap());
		let kv = Nrkv::wrap(Store { fs: self, id }, nrkv::StaticConf);
		Ok(AttrMap(kv))
	}
}
