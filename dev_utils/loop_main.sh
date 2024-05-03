#!/usr/bin/env bash

fd "\\.rs|\\.py" | CARGO_TARGET_DIR=target_bacon RUST_BACKTRACE=1 RUST_LOG=debug entr cargo run --release --bin ppg2_profiling $@
