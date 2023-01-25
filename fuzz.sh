#!/usr/bin/env bash

set -e

TIMEOUT=5s
MAX_LEN=256
CMD="cargo fuzz run random_ops -s none -j $2 -- -timeout=$TIMEOUT -max_len=$MAX_LEN"

if [[ $# != 2 ]]
then
	echo "usage: $0 <crate> <jobs>" >&2
	exit 1
fi

session="fuzz-$1"

tmux new-session -c "$1" -s "$session" -d "$CMD" \; selectl tiled
tmux set remain-on-exit on

tmux attach-session -t "$session"
