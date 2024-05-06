#!/usr/bin/env python

# A quick 'test an interactive instance' script
import sys

sys.path.append("python")
import subprocess  # noqa: E402
import shutil  # noqa: E402
import os  # noqa: E402
import time  # noqa: E402
from pathlib import Path  # noqa: E402

run_path = Path("tests/run/interactive")
if run_path.exists():
    shutil.rmtree(run_path)
run_path.mkdir(exist_ok=True, parents=True)

subprocess.check_call("dev_utils/prep_for_tests.sh")

runtime = 15
try:
    runtime = int(sys.argv[1])
except (ValueError, IndexError):
    pass

print(f"Job will run for {runtime} seconds")

import pypipegraph2 as ppg  # noqa: E402

os.chdir(run_path)

ppg.new()


def do_a(of):
    time.sleep(runtime)
    of.write_text("A")


ppg.FileGeneratingJob("A", do_a)


def do_b(ofs, prefix):
    assert ofs[0].name == "b"
    ofs[0].write_text("B")


b = ppg.SharedMultiFileGeneratingJob("shi", ["b", "B1", "shubidudb"], do_b)


c = ppg.FileGeneratingJob("C", do_a, resources=ppg.Resources.Exclusive)
c.depends_on(b)


ppg.run()
