#!/usr/bin/env bash

. ./_init.sh

touch "$mnt/a"
rm "$mnt/a"

dd if=/dev/urandom of="$mnt/a" bs=128K count=1
rm "$mnt/a"

. ./test/_finish.sh
