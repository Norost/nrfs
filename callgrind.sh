#!/usr/bin/env bash

cargo b --release --bin "$1"
valgrind \
	--tool=callgrind \
	--dump-instr=yes \
	--collect-jumps=yes \
	"./target/release/$1" "${@:2}"
