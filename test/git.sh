#!/usr/bin/env bash

set -xe

# Go to workspace root
script=$(realpath ${BASH_SOURCE[0]})
cd $(dirname $(dirname "$script"))

# Build
cargo b

# Image
img=$(mktemp)
# Mount point
mnt=$(mktemp -d)
trap 'rm -rf "$img" "$mnt"' EXIT

# Format image
fallocate -l 8M "$img"
./target/debug/tool make "$img"

# Mount
./target/debug/fuse "$img" "$mnt" &
trap 'umount "$mnt"; rm -rf "$img" "$mnt"' EXIT

# Wait a bit to ensure the driver is actually running
sleep 0.2
jobs %%

# Make git repo
git init "$mnt"

# Destroy git repo
rm -rf "$mnt/.git"

# Unmount
umount "$mnt"
trap 'rm -rf "$img" "$mnt"' EXIT

# Give driver some time to exit
sleep 0.2

# Dump filesystem
# It should be entirely empty.
./target/debug/tool dump "$img"
