#!/usr/bin/env bash

#BONNIE=$HOME/Documents/benchmark/fs/bonnie++-2.00a/bonnie++
BONNIE=/sbin/bonnie++
MODE=release
IMG_LEN=256M

. ./init.sh

# Run benchmark
set +e
"$BONNIE" -r 256M -d "$mnt"
set -e

find "$mnt"

. ./test/_finish.sh
