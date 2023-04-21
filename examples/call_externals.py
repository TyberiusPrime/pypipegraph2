# check if 'stalling' occurs - ie.
# will having to many all_cores jobs ready to run
# prevent all single cores to run from in parallel
import pypipegraph2 as ppg
import subprocess
import time
from pathlib import Path
import logging
import os
import shutil
import sys

p = Path("run/call_externals")
if p.exists():
    shutil.rmtree(p)
p.mkdir(exist_ok=True, parents=True)
os.chdir(p)

timeout = 1
jobcount = 10
ppg.new(log_level=logging.INFO, cores=5)


def single_cores(ii):
    def inner(of):
        pid = os.fork()
        if pid == 0:
            of.write_bytes(
                subprocess.check_output(f"sleep {timeout} && date", shell=True)
            )
            sys.stdout.close()
            sys.stderr.close()
            time.sleep(1)
        else:
            os.waitpid(pid, 0)

        # time.sleep(timeout)

    return ppg.FileGeneratingJob(
        f"single{ii}",
        inner,
        resources=ppg.Resources.SingleCore,
        depend_on_function=False,
    )


sc = [single_cores(ii) for ii in range(jobcount)]

start = time.time()
ppg.run()
stop = time.time()

print(f"took {stop-start:.2f}.")
