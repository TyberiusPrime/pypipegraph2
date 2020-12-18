#!/usr/bin/env python3
import pypipegraph2 as ppg
import shutil
from pathlib import Path
import os

run_dir = (Path(__file__).parent / "run").absolute()
counter = 0


def setup():
    global counter
    rd = run_dir / str(counter)
    rd.mkdir(parents=True, exist_ok=True)
    os.chdir(rd)
    counter += 1


def simple():
    setup()
    ppg.new()
    jobs = []
    data = {}
    for ii in range(100):

        def load(ii=ii):
            data[ii] = str(ii)

        loader = ppg.DataLoadingJob(f"dl{ii}", load)
        fg = ppg.FileGeneratingJob(
            str(ii),
            lambda of, ii=ii: of.write_text(data[ii]),
            # resources=ppg.Resources.RunsHere,
        )
        fg.depends_on(loader)
        if ii > 10:
            if ii % 2:
                fg.depends_on(jobs[ii % 10])
            else:
                loader.depends_on(jobs[ii % 10])
        jobs.append(fg)
    for i in range(10):
        ppg.run()  # i really want to see the nothing to do parts


def test_simple(benchmark):
    if run_dir.exists():
        shutil.rmtree(run_dir)
    benchmark(simple)


if __name__ == "__main__":
    simple()
