#!/usr/bin/env bash

. ./_init.sh

# Create, build, run & clean cargo project
pushd "$mnt"
cargo init hello
cd hello
cargo r
cargo clean
popd

. ./test/_finish.sh
