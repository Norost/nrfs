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
	#setfattr -n "beep" -v "beep" "$mnt/x" &
	echo "beep" > "$mnt/x" &
	pids="$pids $!"
	echo "beep" > "$mnt/y" &
	pids="$pids $!"
done
for i in $(seq 1 $COUNT)
do
	mv "$mnt/x" "$mnt/y"
	mv "$mnt/y" "$mnt/x"
done
set -x
wait $pids

find "$mnt"

. ./test/_finish.sh
