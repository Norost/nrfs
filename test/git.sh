#!/usr/bin/env bash

. _init.sh

# Make git repo
git init "$mnt"

# Destroy git repo
rm -rf "$mnt/.git"

. ./test/_finish.sh
