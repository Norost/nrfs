#!/usr/bin/env bash

MODE=debug
COUNT=1000
IMG_LEN=256M

. ./_init.sh

touch "$mnt/file"
mknod "$mnt/pipe" p
mknod "$mnt/char" c 0 0
#mknod "$mnt/blk" b 0 0

ls -lah "$mnt"

chmod 124 "$mnt/char"

ls -lah "$mnt"

sync "$mnt"
"./target/$MODE/tool" dump "$img"

. ./test/_finish.sh
