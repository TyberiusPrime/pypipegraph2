#!/usr/bin/env bash
set -eou pipefail

fd "\\.rs|\\.py" | entr ./dev_utils/run_pytest.sh $@
