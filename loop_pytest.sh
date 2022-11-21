#!/usr/bin/env bash

fd "\\.rs|\\.py" | entr ./run_pytest.sh $@
