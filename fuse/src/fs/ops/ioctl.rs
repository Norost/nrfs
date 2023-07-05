use super::*;

const SET_GEN_INTERVAL: u32 = 1;

// <asm/ioctl.h>
const _IOC_NRBITS: u8 = 8;
const _IOC_TYPEBITS: u8 = 8;
const _IOC_SIZEBITS: u8 = 14;
const _IOC_DIRBITS: u8 = 2;

const _IOC_NRMASK: u32 = (1 << _IOC_NRBITS) - 1;
const _IOC_TYPEMASK: u32 = (1 << _IOC_TYPEBITS) - 1;
const _IOC_SIZEMASK: u32 = (1 << _IOC_SIZEBITS) - 1;
const _IOC_DIRMASK: u32 = (1 << _IOC_DIRBITS) - 1;

const _IOC_NRSHIFT: u8 = 0;
const _IOC_TYPESHIFT: u8 = _IOC_NRSHIFT + _IOC_NRBITS;
const _IOC_SIZESHIFT: u8 = _IOC_TYPESHIFT + _IOC_TYPEBITS;
const _IOC_DIRSHIFT: u8 = _IOC_SIZESHIFT + _IOC_SIZEBITS;

const _IOC_NONE: u32 = 0;
const _IOC_WRITE: u32 = 1;
const _IOC_READ: u32 = 2;

fn _ioc_dir(nr: u32) -> u32 {
	(nr >> _IOC_DIRSHIFT) & _IOC_DIRMASK
}
fn _ioc_type(nr: u32) -> u32 {
	(nr >> _IOC_TYPESHIFT) & _IOC_TYPEMASK
}
fn _ioc_nr(nr: u32) -> u32 {
	(nr >> _IOC_NRSHIFT) & _IOC_NRMASK
}
fn _ioc_size(nr: u32) -> u32 {
	(nr >> _IOC_SIZESHIFT) & _IOC_SIZEMASK
}

impl Fs {
	pub async fn ioctl(&self, job: crate::job::IoCtl) {
		match _ioc_nr(job.cmd) {
			SET_GEN_INTERVAL => {
				let Ok(t) = <[u8; 8]>::try_from(&*job.in_data) else {
					return job.reply.error(libc::EINVAL);
				};
				let t = i64::try_from(u64::from_le_bytes(t) / 1000).unwrap().max(1);
				self.gen_interval.set(t);
				job.reply.ioctl(0, &[]);
			}
			_ => job.reply.error(libc::EINVAL),
		}
	}
}
