#!/usr/bin/env bash

set -xe

export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code -Coverflow-checks=off -Zpanic_abort_tests -Cpanic=abort"
export RUSTDOCFLAGS="-Cpanic=abort"

cargo +nightly t -p nros --profile grcov

grcov ./target/grcov/ \
	-s . \
	--binary-path ./target/grcov \
	-t html \
	--llvm \
	--branch \
	--ignore-not-existing \
	-o ./target/grcov/coverage/

xdg-open target/grcov/coverage/index.html
