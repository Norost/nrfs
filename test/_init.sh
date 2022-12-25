#!/usr/bin/env bash

set -xe

# Go to workspace root
script=$(realpath ${BASH_SOURCE[0]})
cd $(dirname $(dirname "$script"))

# Build
if test "$MODE" = "release"
then
	cargo b --release $CARGO_FLAGS
else
	cargo b $CARGO_FLAGS
fi

# Image
img=$(mktemp)
# Mount point
mnt=$(mktemp -d)
trap 'rm -rf "$img" "$mnt"' EXIT

# Format image
if test -z "$IMG_LEN"
then
	IMG_LEN=8M
fi
fallocate -l "$IMG_LEN" "$img"
./target/debug/tool make "$img"

# Mount
./target/debug/fuse "$img" "$mnt" &
trap 'umount "$mnt"; ./target/debug/tool dump "$img"; rm -rf "$img" "$mnt"' EXIT

# Wait a bit to ensure the driver is actually running
sleep 0.2
jobs %%
