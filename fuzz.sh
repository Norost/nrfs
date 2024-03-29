#!/usr/bin/env bash

set -e

TIMEOUT=30s
MAX_LEN="${MAX_LEN:-8192}"
MAX_RSS="${MAX_RSS:-512}"
CMD="nice cargo fuzz run random_ops -s none -- -timeout=$TIMEOUT -max_len=$MAX_LEN -rss_limit_mb=$MAX_RSS -len_control=500"

if [[ $# != 2 ]]
then
	echo "usage: $0 <crate> <jobs>" >&2
	exit 1
fi

session="fuzz-$1"

cd "$1"

# Minimize corpus first.
cargo fuzz cmin random_ops -s none

tmux new-session -s "$session" -d "$CMD" \
	\; selectl tiled \
	\; set-option history-limit 10000 \
	\; set remain-on-exit on

for i in $(seq 2 "$2")
do
    tmux split-window -t "$session" -d "$CMD" \
		\; selectl tiled \
		\; set-option history-limit 10000
done

tmux attach-session -t "$session"
