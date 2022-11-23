#!/usr/bin/env bash

./prep_for_tests.sh
echo "cargo test"
cargo test
echo "pytest"
pytest $@
