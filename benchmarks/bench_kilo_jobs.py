#!/usr/bin/env python3
import pypipegraph2 as ppg
from pathlib import Path
import os

run_dir = (Path(__file__).parent / "run_kilojobs").absolute()
run_dir.mkdir(exist_ok=True)
counter = 0


def gen_job(ii):
    return ppg.FileGeneratingJob(f"{ii}.dat", lambda of: of.write_text("h"))


def kilo_jobs():
    os.chdir(run_dir)
    ppg.new()
    final = ppg.FileGeneratingJob("final.dat", lambda of: of.write_text("h"))

    for ii in range(100000):
        final.depends_on(gen_job(ii))
    ppg.run()


if __name__ == "__main__":
    kilo_jobs()


# just doing it: 45s. (183k history)
# noop rerun: 11s
# rerun, rebuild final:  24s


# after dicodon fixes
# noop rerun:   22s
# rerrun with rebuild final:  9s


# 100k
# run : 25m35s. 1.8M (compressed) history. 81MB uncompressed...
# noop run: 1minute.
