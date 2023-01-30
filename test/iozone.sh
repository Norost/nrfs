#!/usr/bin/env bash

IOZONE=iozone
MODE=release
IMG_LEN=4G

. ./_init.sh

# Run benchmark
pushd "$mnt"
set +e
"$IOZONE" -a
set -e
popd

find "$mnt"

. ./test/_finish.sh
