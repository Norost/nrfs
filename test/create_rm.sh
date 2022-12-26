#!/usr/bin/env bash

MODE=debug
COUNT=1000
IMG_LEN=256M

. ./_init.sh

# Test recursive delete shit
mkdir "$mnt/test"
for i in $(seq 1 $COUNT)
do
	touch "$mnt/test/$i"
done
strace rm -r "$mnt/test" 2> /tmp/rm_r.txt

find "$mnt"

. ./test/_finish.sh
