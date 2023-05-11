#!/usr/bin/env bash

. ./_init.sh

touch "$mnt/a"
getfattr "$mnt/a"
setfattr -n "hey" -v "ho" "$mnt/a"
setfattr -n "system.bro" "$mnt/a"
setfattr -n "security.ohno" "$mnt/a" && exit 1
setfattr -n "nrfs.reserved" "$mnt/a" && exit 1
getfattr -m - -d "$mnt/a"
sync "$mnt"
./target/debug/tool dump "$img"

. ./test/_finish.sh
