# check if 'stalling' occurs - ie.
# will having to many all_cores jobs ready to run
# prevent all single cores to run from in parallel
import pypipegraph2 as ppg
import time
from pathlib import Path
import logging
import os
import shutil

p = Path("run/stall")
if p.exists():
    shutil.rmtree(p)
p.mkdir(exist_ok=True, parents=True)
os.chdir(p)

timeout = 2
jobcount = 5
ppg.new(log_level=logging.INFO, cores=5)


def all_cores(ii):
    def inner(of):
        of.write_text(str(time.time()))
        time.sleep(timeout)

    return ppg.FileGeneratingJob(
        f"all_cores{ii}",
        inner,
        resources=ppg.Resources.AllCores,
        depend_on_function=False,
    )


def single_cores(ii):
    def inner(of):
        of.write_text(str(time.time()))
        time.sleep(timeout)

    return ppg.FileGeneratingJob(
        f"single{ii}",
        inner,
        resources=ppg.Resources.SingleCore,
        depend_on_function=False,
    )


sc = [single_cores(ii) for ii in range(jobcount)]
ac = [all_cores(ii) for ii in range(jobcount)]

start = time.time()
ppg.run()
stop = time.time()

print(
    f"took {stop-start:.2f}. If this is close to {timeout * jobcount}, all is well. "
    "If it's more, we have an ordering problem"
)
