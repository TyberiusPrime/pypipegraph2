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
counter = 0

try:
    count = int(sys.argv[1])
except:
    count = 200


def setup():
    global counter
    rd = run_dir / str(counter)
    rd.mkdir(parents=True, exist_ok=True)
    os.chdir(rd)
    counter += 1


def simple():
    setup()
    ppg.new()

    data = {}

    def final(of):
        of.write_text(str(len(data)))

    final_job = ppg.FileGeneratingJob("final", final)

    for ii in range(count):

        def load(ii=ii):
            data[ii] = str(ii)

        loader = ppg.DataLoadingJob(f"dl{ii}", load)
        final_job.depends_on(loader)
    ppg.run()


def test_simple(benchmark):
    if run_dir.exists():
        shutil.rmtree(run_dir)
    benchmark(simple)


if __name__ == "__main__":
    simple()
