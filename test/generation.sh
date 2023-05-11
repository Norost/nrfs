#!/usr/bin/env bash

. ./_init.sh

IOCTL_BUMP_GEN=1

mkdir -p "$mnt/a/b/c"
touch "$mnt/a/x"
touch "$mnt/a/b/y"
find "$mnt" -exec getfattr -d -e hex -m nrfs.gen {} +
sync "$mnt"
./target/debug/tool dump "$img"

python3 -c "import fcntl, os; fcntl.ioctl(os.open('$mnt', os.O_RDONLY), $IOCTL_BUMP_GEN)"
touch "$mnt/a/b/c/z"
find "$mnt" -exec getfattr -d -e hex -m nrfs.gen {} +
sync "$mnt"
./target/debug/tool dump "$img"

. ./test/_finish.sh
