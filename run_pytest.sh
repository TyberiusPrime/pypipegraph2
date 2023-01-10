#!/usr/bin/env bash

./prep_for_tests.sh
echo "cargo test"
CARGO_TARGET_DIR=target_bacon cargo test
echo "pytest"
CARGO_TARGET_DIR=target_bacon RUST_LOG=debug pytest $@
