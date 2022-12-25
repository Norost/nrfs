#!/usr/bin/env bash

# Unmount
umount "$mnt"
if test -n "$DUMP_FS"
then
	trap '"./target/$MODE/tool" dump "$img"; rm -rf "$img" "$mnt"' EXIT
else
	trap 'rm -rf "$img" "$mnt"' EXIT
fi

# Give driver some time to exit
sleep 0.2
