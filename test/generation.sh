#!/usr/bin/env bash

. ./_init.sh

IOCTL_BUMP_GEN=1

mkdir -p "$mnt/a/b/c"
touch "$mnt/a/x"
touch "$mnt/a/b/y"
find "$mnt" -exec getfattr -d -e hex -m nrfs.gen {} +
sync "$mnt"
./target/debug/tool dump "$img"

# ioctl() really does suck ASS
python3 -c "$(cat <<-EOF
	import fcntl, os
	t = b'\x00\xca\x9a\x3b\0\0\0\0' # 1 second, in nanos, little-endian
	print(t)

	IOCTL_SET_GEN_INTERVAL = 1
	
	NRBITS = 8
	TYPEBITS = 8
	SIZEBITS = 14
	DIRBITS = 2

	NRSHIFT = 0
	TYPESHIFT = NRSHIFT + NRBITS
	SIZESHIFT = TYPESHIFT + TYPEBITS
	DIRSHIFT = SIZESHIFT + SIZEBITS

	_IOC_WRITE = 1
	_IOC = lambda dir,type,nr,size: (dir << DIRSHIFT) | (type << TYPESHIFT) | (nr << NRSHIFT) | (size << SIZESHIFT)
	_IOW = lambda type,nr,size: _IOC(_IOC_WRITE, type, nr, size)

	req = _IOW(_IOC_WRITE, IOCTL_SET_GEN_INTERVAL, len(t))
	fcntl.ioctl(os.open('$mnt', os.O_RDONLY), req, t, False)
EOF
)"
touch "$mnt/a/b/c/z"
find "$mnt" -exec getfattr -d -e hex -m nrfs.gen {} +
sync "$mnt"
./target/debug/tool dump "$img"

. ./test/_finish.sh
