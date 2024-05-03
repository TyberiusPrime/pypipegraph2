#!/usr/bin/env bash
set -eou pipefail

fd "\\.rs|\\.py" | entr ./run_pytest.sh $@
