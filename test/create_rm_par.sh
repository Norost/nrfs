#!/usr/bin/env bash

COUNT=1000
IMG_LEN=256M
MAKE_ARGS="-b 9 -r 9"
FUSE_ARGS="--cache-size $((512 * 1))"

. ./_init.sh

# Test recursive delete shit
mkdir "$mnt/test"
pids=
set +x
for i in $(seq 1 $COUNT)
do
	touch "$mnt/test/$i" &
	pids="$pids $!"
done
set -x
wait $pids
strace rm -r "$mnt/test" 2> /tmp/rm_r.txt

find "$mnt"

. ./test/_finish.sh
