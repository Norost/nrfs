#!/usr/bin/env bash

set -e

TIMEOUT=5s
MAX_LEN="${MAX_LEN:-4096}"
CMD="cargo fuzz run random_ops -s none -- -timeout=$TIMEOUT -max_len=$MAX_LEN"

if [[ $# != 2 ]]
then
	echo "usage: $0 <crate> <jobs>" >&2
	exit 1
fi

session="fuzz-$1"

tmux new-session -c "$1" -s "$session" -d "$CMD" \
	\; selectl tiled \
	\; set-option history-limit 10000 \
	\; set remain-on-exit on

for i in $(seq 2 "$2")
do
    tmux split-window -c "$1" -t "$session" -d "$CMD" \
		\; selectl tiled
done

tmux attach-session -t "$session"
