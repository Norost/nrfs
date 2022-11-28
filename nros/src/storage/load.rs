use super::{Store, dev, Allocator, Storage};
use crate::{NewError, MaxRecordSize, Compression, header::Header};

impl<D, A> Store<D, A>
where
	D: Dev,
	A: dev::Allocator<Error = D::Error>,
{
	pub fn load<I>(
		storage: I,
		alloc: A,
	) -> Result<Self, LoadError<D>>
	where
		I: IntoIterator<Item = D>,
	{
		for d in storage {
			d.read
		}

		Ok(Self {
			allocator: Allocator::load(&mut storage, alloc_log.lba, alloc_log.len)?,
			storage,
			max_record_size,
			compression,
			alloc,
		})
	}
}

pub struct LoadStore<D, A>
where
	D: dev::Dev,
	A: dev::Allocator<Error = D::Error>,
{
	store: Store<D, A>,
	state: State<D>,
}

impl<D, A> LoadStore<D, A>
where
	D: dev::Dev,
	A: dev::Allocator<Error = D::Error>,
{
	pub fn poll(mut self) -> Result<Poll<Store<D, A>, Self>, LoadError<D>> {
		match &mut self.state {
			State::ReadHeaders { tail, tokens } => {
				for (e, d) in tokens.iter_mut().zip(&mut self.store.storage) {
					match e.take().map(|e| d.dev.poll_read(e)).transpose().map_err(LoadError::Dev)? {
						Some(dev::Task::Done(buf)) => {
							let l = d.header.as_ref().len();
							d.header.as_mut().copy_from_slice(&buf.get()[..l]);
							if !d.header.verify_xxh3() {
								// Try the head headers instead.
								// If that fails, fall back to mirrors
								// If that fails, RIP
								todo!();
							}
						}
						t => *e = t,
					}
				}
				if t.iter().all(|e| e.is_none()) {
					// Check if UIDs match and properties are sane
					let base = &self.store.storage[0].header;
					for d in &self.store.storage[1..] {
						if !base.compatible(&d.header) {
							// If the tail headers are inconsistent the filesystem may have
							// been interrupted during a transaction.
							// Try the head headers.
							// If that fails, fall back to mirrors
							// If that fails, RIP
							todo!();
						}
					}

					t.clear();
					for d in self.store.storage.iter_mut() {
						d.save(false, &mut self.store.alloc)?.map(|e| t.push(e));
					}
					*self.state = State::FenceHead(t);
				}
			}
			State::FenceHead(t) => {
				for (e, d) in t.iter_mut().zip(&mut self.store.storage) {
					*e = e.take().map(|e| d.dev.poll_fence(e)).transpose()?;
				}
				if t.iter().all(|e| e.is_none()) {
					return Ok(Poll::Done(self.store));
				}
			}
		}
		Ok(Poll::Wait(self))
	}
}

enum State<D>
where
	D: dev::Dev,
{
	ReadHeaders {
		tail: bool,
		tokens: Vec<Option<D::ReadToken>>,
	},
	ReadAllocLog {
		state: (),
		token: D::ReadToken,
	},
}

pub enum Poll<T, F> {
	Done(T),
	Wait(F),
}
