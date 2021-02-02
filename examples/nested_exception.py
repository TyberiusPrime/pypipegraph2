# showcase how a nested exception will look like
import pypipegraph2 as ppg
from pathlib import Path
import os
import shutil

p = Path("run/nested_exceptions")
if p.exists():
    shutil.rmtree(p)
p.mkdir(exist_ok=True, parents=True)
os.chdir(p)


def a():
    try:
        raise ValueError()
    except ValueError as e:
        raise KeyError() from e


ppg.DataLoadingJob("a", a)
try:
    ppg.run()
except ppg.RunFailed:
    pass


try:
    raise ValueError()
except ValueError:
    raise KeyError()
