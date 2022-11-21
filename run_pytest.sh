#!/usr/bin/env bash

export CARGO_TARGET_DIR=target
cargo build
cp target/debug/libppg2_rust.so python/ppg2_rust/ppg2_rust.cpython-39-x86_64-linux-gnu.so
cargo test
pytest $@
