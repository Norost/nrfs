use {
	super::Dir,
	crate::{Dev, EnableExt, Error, Ext, Nrfs},
};

impl<'a, D: Dev> Dir<'a, D> {
	/// Create a new directory.
	pub(crate) async fn init(fs: &'a Nrfs<D>, ext: EnableExt) -> Result<u64, Error<D>> {
		let dir = fs.storage.create().await?;
		let heap_id = fs.storage.create().await?.id();

		let mut hdr = [0; 128];
		hdr[8..16].copy_from_slice(&heap_id.to_le_bytes());
		let mut offt = 32;
		let mut item_offt = 0u16;

		let mut add_ext = |ext, hdr_len, item_len| {
			hdr[offt] = fs.get_id_or_insert(ext).0.get();
			hdr[offt + 1..offt + 1 + hdr_len].copy_from_slice(&item_offt.to_le_bytes());
			offt += 1 + hdr_len;
			item_offt += item_len;
		};
		ext.unix().then(|| add_ext(Ext::Unix, 2, 8));
		ext.mtime().then(|| add_ext(Ext::MTime, 2, 8));

		dir.write(0, &hdr).await?;

		Ok(dir.id())
	}
}
