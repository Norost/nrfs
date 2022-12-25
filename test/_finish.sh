#!/usr/bin/env bash

# Unmount
umount "$mnt"
trap './target/debug/tool dump "$img"; rm -rf "$img" "$mnt"' EXIT

# Give driver some time to exit
sleep 0.2
