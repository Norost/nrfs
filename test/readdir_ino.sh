#!/usr/bin/env bash

. ./_init.sh

gcc test/check_ino.c -o test/check_ino

touch "$mnt"/{a,b,c,d,e}

# Requires root, alas
#
# remount should cause *all* inodes to be forgotten.
# To check, run with RUST_LOG=trace
mount -i -oremount "$mnt"

./test/check_ino "$mnt"
ls -i "$mnt"
./test/check_ino "$mnt"

. ./test/_finish.sh
