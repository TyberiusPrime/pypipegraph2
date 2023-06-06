from pathlib import Path
import pypipegraph2 as ppg
import time
import psutil
import shutil
import os
import subprocess
import setproctitle

print("stage 2 pid is ", os.getpid())

setproctitle.setproctitle("stage2")

print("the ppg runner pid is", os.getpid())
p = Path("shu_test")
if p.exists():
    shutil.rmtree(p)
p.mkdir()
os.chdir(p)

Path("out").mkdir()
ppg.new()


def do_a(of):
    Path("a.txt").write_text(f"from do_a {os.getpgid(0)}")
    setproctitle.setproctitle("do_a")
    subprocess.check_call(
        [Path(__file__).parent / "sleeper.sh", "ppg2_test_parallel_sentinel"]
    )


def kill_us():
    time.sleep(1)

    p = psutil.Process(os.getpid())
    ppg.util.log_error(f"now killing this jobs process. {os.getpid()}")
    p.kill()


jobA = ppg.FileGeneratingJob("A", do_a)
jobB = ppg.JobGeneratingJob("B", kill_us)

try:
    ppg.run()
except ppg.FatalGraphException as e:
    if "killed" in str(e):
        pass
    else:
        raise


print("all done")
