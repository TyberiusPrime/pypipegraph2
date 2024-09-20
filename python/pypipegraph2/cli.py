"""CLI interface to ppg2"""

import pyzstd
import re
import sys
import pypipegraph2 as ppg2


def filter_constraint_violations(filename=None, pipegraph=None):
    """For when you really need to remove some jobs from the pipegraph's history.
    Takes a filename with job ids.
    Defaults to .ppg/errors/latest/constraint_violations.jobs

    If you have your ppg history in a non standard place, pass in a ppg
    with the correct directories set.
    """
    if '--help' in sys.argv:
        print("ppg2-cli filter_constraint_violations [filename] - filter constraint violations from history")
    if pipegraph is None:
        pipegraph = ppg2.new()

    if sys.argv[1:]:
        filename = sys.argv[1]
    if filename is None:
        filename = pipegraph.dir_config.error_dir / "latest/constraint_violations.jobs"

    q = filename.read_text().strip().split("\n")
    print("reading history")
    history = pipegraph._load_history()

    to_del = set()
    for key, v in history.items():
        found = False
        for x in q:
            if key.startswith(x) or key.endswith(x):
                print("Would delete")
                print("\thistory: ", key)
                print("\tbecause: ", x)
                print("")
                to_del.add(key)
                found = True
            if found:
                break
    print(f"Delete {len(to_del)} history entries?")
    print("type yes<enter> to proceed")
    print("This tool does not create backups. Make a snapshot of your project first")
    for x in to_del:
        del history[x]
    if input() == "yes":
        ppg2.global_pipegraph._save_history(history)
    else:
        print("aborted")
