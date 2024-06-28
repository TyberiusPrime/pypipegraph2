#!/usr/bin/env bash
# Prepare for python tests by copying the so where we need it
set -eou pipefail
CARGO_TARGET_DIR=target cargo build --release
cp target/release/libpypipegraph2.so python/pypipegraph2/pypipegraph2.cpython-39-x86_64-linux-gnu.so
cp target/release/libpypipegraph2.so python/pypipegraph2/pypipegraph2.cpython-310-x86_64-linux-gnu.so
cp target/release/libpypipegraph2.so python/pypipegraph2/pypipegraph2.cpython-311-x86_64-linux-gnu.so
cp target/release/libpypipegraph2.so python/pypipegraph2/pypipegraph2.cpython-312-x86_64-linux-gnu.so
# echo "Ready"



