#!/usr/bin/env bash
set -eou pipefail
export CARGO_TARGET_DIR=target_pyloop c 
echo "cargo test"
cargo test
dev_utils/prep_for_tests.sh
echo "pytest"
# turn python into absolute path
PYTHONPATH=$(pwd)/python RUST_BACKTRACE=1 RUST_LOG=debug pytest $@
