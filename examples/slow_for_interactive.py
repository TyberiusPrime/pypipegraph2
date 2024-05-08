# demonstrate runtime logging
import sys
import pypipegraph2 as ppg
import time
from pathlib import Path
import os
import shutil

p = Path("run/slow_running")
if len(sys.argv) <= 1:
    print("Nuking existing", p)
    if p.exists():
        shutil.rmtree(p)
p.mkdir(exist_ok=True, parents=True)
os.chdir(p)


total_runtime = 10

ppg.new(cores=3, run_mode=ppg.RunMode.CONSOLE)


def gen_jobs_stack(name, runtime, total_runtime=total_runtime):
    def inner(of, runtime=runtime):
        for ii in range(runtime):
            time.sleep(1)
        if "_10" in of.name:
            raise ValueError("expected")
        of.write_text(str(of) + "_" + str(runtime))

    out_jobs = []
    for ii in range(0, total_runtime + 1, runtime):
        ppg.FileGeneratingJob(f"{name}_{ii}", inner, resources=ppg.Resources.SingleCore)
        if out_jobs:
            out_jobs[-1].depends_on(ii)
    return out_jobs


# gen_jobs_stack("1s", 1)
gen_jobs_stack("30s", 20)
# gen_jobs_stack('60s', 60)


def gen_2nd():
    gen_jobs_stack("1sB", 1, 5)


ppg.JobGeneratingJob("genjobs", gen_2nd)

try:
    ppg.run()
except ppg.RunFailed:
    pass
