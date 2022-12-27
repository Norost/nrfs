#!/usr/bin/env bash

CMD="cargo fuzz run random_ops -s none"

if [[ $# != 2 ]]
then
	echo "usage: $0 <crate> <jobs>" >&2
	exit 1
fi

session="fuzz-$1"

tmux new-session -c "$1" -s "$session" -d "$CMD" \; selectl tiled

for i in $(seq 2 "$2")
do
	tmux split-window -c "$1" -t "$session" -d "$CMD" \; selectl tiled
done

tmux attach-session -t "$session"
