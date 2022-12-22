#!/usr/bin/env bash

MODE=release

set -xe

# Go to workspace root
script=$(realpath ${BASH_SOURCE[0]})
cd $(dirname $(dirname "$script"))

# Build
if test "$MODE" = "release"
then
	cargo b --release
else
	cargo b
fi

# Image
img=$(mktemp)
# Mount point
mnt=$(mktemp -d)
trap 'rm -rf "$img" "$mnt"' EXIT

# Format image
fallocate -l 256M "$img"
./target/$MODE/tool make "$img"

# Mount
./target/$MODE/fuse "$img" "$mnt" &
trap 'umount "$mnt"; rm -rf "$img" "$mnt"' EXIT

# Wait a bit to ensure the driver is actually running
sleep 0.2
jobs %%

# Test recursive delete shit
mkdir "$mnt/test"
for i in $(seq 1 1000000)
do
	touch "$mnt/test/$i"
done
strace rm -r "$mnt/test" 2> /tmp/rm_r.txt

find "$mnt"

# Unmount
umount "$mnt"
trap 'rm -rf "$img" "$mnt"' EXIT

# Give driver some time to exit
sleep 0.2

# Dump filesystem
./target/$MODE/tool dump "$img"
