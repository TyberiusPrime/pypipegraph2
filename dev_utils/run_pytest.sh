#!/usr/bin/env bash
set -eou pipefail
export CARGO_TARGET_DIR=target_pyloop c 
echo "cargo test"
cargo test
dev_utils/prep_for_tests.sh
echo "pytest"
PYTHONPATH=python RUST_BACKTRACE=1 RUST_LOG=debug pytest $@
