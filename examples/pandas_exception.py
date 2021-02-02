# pandas exceptions have 'no source available' sections in their tracebacks
import pypipegraph2 as ppg
from pathlib import Path
import os
import shutil
import pandas

p = Path("run/pandas_excetion")
if p.exists():
    shutil.rmtree(p)
p.mkdir(exist_ok=True, parents=True)
os.chdir(p)


def a():
    try:
        df = pandas.DataFrame()
        df["shu"]
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
