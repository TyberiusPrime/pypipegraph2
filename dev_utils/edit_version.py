#!/usr/bin/env python
# open all files that need to be changed for version changes in nvim
#
import tomllib
import subprocess
from pathlib import Path
import re

files = ["python/pypipegraph2/__init__.py", "Cargo.toml", "setup.cfg", "pyproject.toml"]

old_version = tomllib.load(open("Cargo.toml", "rb"))["package"]["version"]

error = False

for fn in files:
    old = Path(fn).read_text()
    matches = re.findall(old_version.replace(".", "\\."), old)
    if len(matches) != 1:
        print(
            f"Version {old_version} found the wrong number of times ({len(matches)}) in {fn}"
        )
        error = True

if error:
    print("You will have to edit manually.")
    input("press enter")
    subprocess.check_call(["nvim"] + files)
else:
    print(f"Old version {old_version} found in relevant all files exactly once.")
    print("going to autoreplace")
    new_version = input("please enter new version: ")
    for fn in files:
        old = Path(fn).read_text()
        new = old.replace(old_version, new_version)
        Path(fn).write_text(new)
        print(f"Replaced in {fn}")

    subprocess.check_call(['cargo','check'])
