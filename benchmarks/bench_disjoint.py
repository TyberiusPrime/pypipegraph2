#!/usr/bin/env python3

"""This creates one FileGeneratingJob with a few hundred of direct DataLoadingJobs,
which exposed a o(n**3) or so bottleneck in runner.modify_dag
"""
import pypipegraph2 as ppg
import shutil
from pathlib import Path
import os
import sys

run_dir = (Path(__file__).parent / "run").absolute()

try:
    count = int(sys.argv[1])
except:  # noqa:E722
    count = 200


def simple():
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir()
    os.chdir(run_dir)
    ppg.new()

    for ii in range(count):
        a = ppg.FileGeneratingJob(f"file_{ii}", lambda of: of.write_text(of.name))
        b = ppg.DataLoadingJob(f"load_{ii}", lambda ii=ii: ii)
        a.depends_on(b)
    ppg.run()


def test_simple(benchmark):
    print(run_dir)
    benchmark(simple)


if __name__ == "__main__":
    simple()
