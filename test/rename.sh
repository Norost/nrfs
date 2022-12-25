#!/usr/bin/env bash

. ./_init.sh

mkdir "$mnt/test"
touch "$mnt/test/a"
mv "$mnt/test/a" "$mnt/test/b"
sync "$mnt/test/b"
ls -lah "$mnt/test/a" || echo -n
ls -lah "$mnt/test/b"
find "$mnt"
./target/debug/tool dump "$img"

. ./test/_finish.sh
