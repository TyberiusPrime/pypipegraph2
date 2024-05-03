#!/usr/bin/env bash

export CARGO_TARGET_DIR=target_pyloop c 
./prep_for_tests.sh
echo "cargo test"
cargo test
echo "pytest"
RUST_BACKTRACE=1 RUST_LOG=debug pytest $@
