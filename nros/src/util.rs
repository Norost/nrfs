/// Read data at the given offset from `r`, filling data not present in `r` with zeroes.
pub fn read_from(r: &[u8], offt: usize, buf: &mut [u8]) {
	if offt >= r.len() {
		buf.fill(0);
		return;
	}
	let i = r.len().min(offt + buf.len());
	let (l, h) = buf.split_at_mut(i - offt);
	l.copy_from_slice(&r[offt..][..l.len()]);
	h.fill(0);
}

/// Write data to `w`, growing `w` if necessary.
pub fn write_to(w: &mut Vec<u8>, offt: usize, data: &[u8]) {
	if offt + data.len() > w.len() {
		w.resize(offt + data.len(), 0);
	}
	w[offt..][..data.len()].copy_from_slice(data);
}
