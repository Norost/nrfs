#!/bin/bash

if rg 'select!' -g '*.rs'
then
	echo 'Do not use `futures_util::select!`' >&2
	echo 'It is pseudo-random and hence not deterministic' >&2
	echo 'This makes it very difficult to reproduce failures' >&2
	echo 'Use `futures_util::select_biased!` instead' >&2
	exit 1
fi
