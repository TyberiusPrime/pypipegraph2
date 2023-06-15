import subprocess
import sys
import os
from pathlib import Path
import psutil

import setproctitle

setproctitle.setproctitle("stage1")


p = subprocess.Popen(["python", Path(__file__).parent / "stage2.py"], env=os.environ)
p.communicate()
print("stage 2 ended, ", os.getpid())
subprocess.check_call(["ps", "--forest", "-o", "%p %r %c"])

for proc in psutil.process_iter(["cmdline"]):
    if "ppg2_test_parallel_sentinel" in proc.info["cmdline"]:
        print(
            "found a ppg2_test_parallel_sentinel", "pid", proc.pid, "ppid", proc.ppid()
        )
        sys.exit(1)
    if "sleep" in proc.info["cmdline"]:
        print("found a sleep", "pid", proc.pid, "ppid", proc.ppid())
        print(proc.pid, proc.ppid())
        sys.exit(1)

sys.exit(0)
