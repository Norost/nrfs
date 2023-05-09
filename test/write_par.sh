#!/usr/bin/env bash

COUNT=1000
IMG_LEN=256M
MAKE_ARGS="-b 9 -r 9"
FUSE_ARGS="--cache-size $((512 * 1))"

. ./_init.sh

pids=
set +x
for i in $(seq 1 $COUNT)
do
	echo "$i" > "$mnt/$i" &
	pids="$pids $!"
done
set -x
wait $pids

. ./test/_finish.sh
