# demonstrate runtime logging
import pypipegraph2 as ppg
import time
from pathlib import Path
import os
import shutil

p = Path("run/runtimes")
if p.exists():
    shutil.rmtree(p)
Path("run/runtimes").mkdir(exist_ok=True, parents=True)
os.chdir("run/runtimes")


def gen_job(name, runtime):
    def inner(fn):
        time.sleep(runtime)
        fn.write_text(str(runtime))

    return ppg.FileGeneratingJob(name, inner)


gen_job("A1", 1)
gen_job("A.5", 0.5)
gen_job("A2", 2)
gen_job("A5", 5)
ppg.run()
#
