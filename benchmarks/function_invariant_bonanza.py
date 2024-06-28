#!/usr/bin/env python3
import pypipegraph2 as ppg
from pathlib import Path
import os


run_dir = (Path(__file__).parent / "run_kilojobs").absolute()
run_dir.mkdir(exist_ok=True)
counter = 0


def func1():
    pass


def func2():
    pass


def func3():
    pass


def gen_job(ii):
    return (
        ppg.DataLoadingJob(f"{ii}.dat", lambda: 5)  # lambda of: of.write_text("h"))
        .depends_on_func(func1, name=f"{ii}-Func1")
        .self.depends_on_func(func2, name=f"{ii}-Func2")
        .self.depends_on_func(func3, name=f"{ii}-Func3")
        .self.depends_on_func(func3, name=f"{ii}-Func4")
        .self.depends_on_func(func3, name=f"{ii}-Func5")
        .self.depends_on_func(func3, name=f"{ii}-Func6")
        # .self.depends_on_func(func3, name=f"{ii}-Func7")
        # .self.depends_on_func(func3, name=f"{ii}-Func8")
        # .self.depends_on_func(func3, name=f"{ii}-Func9")
        # .self.depends_on_func(func3, name=f"{ii}-Func10")
        # .self.depends_on_func(func3, name=f"{ii}-Func11")
        .self
    )


def kilo_jobs():
    os.chdir(run_dir)
    ppg.new()
    final = ppg.FileGeneratingJob("final.dat", lambda of: of.write_text("h"))

    for ii in range(2000):
        final.depends_on(gen_job(ii))
    ppg.run()


if __name__ == "__main__":
    print("startup")
    kilo_jobs()


# just doing it: 45s. (183k history)
# noop rerun: 11s
# rerun, rebuild final:  24s


# after dicodon fixes
# noop rerun:   22s
# rerrun with rebuild final:  9s


# 100k
# run : 25m35s. 1.8M (compressed) history. 81MB uncompressed...
#
