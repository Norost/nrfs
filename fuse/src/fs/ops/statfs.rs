use super::*;

impl Fs {
	pub async fn statfs(&self, job: crate::job::StatFs) {
		let stat = self.fs.statistics();
		let store = &stat.object_store.storage;
		let alloc = &store.allocation;
		job.reply.statfs(
			alloc.total_blocks,
			alloc.total_blocks - alloc.used_blocks,
			alloc.total_blocks - alloc.used_blocks,
			u64::MAX,
			u64::MAX - stat.object_store.used_objects,
			1 << store.block_size.to_raw(),
			255,
			0,
		);
	}
}
