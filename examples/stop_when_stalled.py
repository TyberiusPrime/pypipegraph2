# check if we can stop whilst stalling
# only one job should finish.
import pypipegraph2 as ppg
import time
from pathlib import Path
import logging
import os
import shutil
import psutil
import subprocess

p = Path("run/stall")
if p.exists():
    shutil.rmtree(p)
p.mkdir(exist_ok=True, parents=True)
os.chdir(p)

time_before_abort = 1
timeout = 1
jobcount = 5
ppg.new(log_level=logging.INFO, cores=5)
# tell the ppg to accept the SIGINT we're actually sending
ppg.global_pipegraph._debug_allow_ctrl_c = "stop"


def all_cores(ii):
    if ii == 3:
        return ppg.MultiFileGeneratingJob(
            ["A", "B", "C"],
            lambda ofs: [of.write_text(of.name) for of in ofs],
            resources=ppg.Resources.AllCores,
        )

    def inner(of, ii=ii):
        of.write_text(str(time.time()))
        proc = psutil.Process()
        parent = proc.parent()
        if ii == 0:  # only the first guy kills us
            time.sleep(time_before_abort)
            subprocess.check_call(["kill", "--signal", "SIGINT", str(parent.pid)])

        time.sleep(timeout)

    return ppg.FileGeneratingJob(
        f"all_cores{ii}",
        inner,
        resources=ppg.Resources.AllCores,
        depend_on_function=False,
    )


ac = [all_cores(ii) for ii in range(jobcount)]

start = time.time()
try:
    ppg.run()
except KeyboardInterrupt:
    print("Received expected Keyboard interrupt")
    stop = time.time()
    print(
        f"stop took {stop-start-time_before_abort:.2f} seconds.\n "
        "We expected {timeout}.\nNot multiples."
    )
