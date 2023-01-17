#!/usr/bin/env bash
CARGO_TARGET_DIR=target cargo build
cp target/debug/libpypipegraph2.so python/pypipegraph2/pypipegraph2.cpython-39-x86_64-linux-gnu.so
echo "Ready"



