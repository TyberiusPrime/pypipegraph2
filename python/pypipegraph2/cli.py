"""CLI interface to ppg2"""

import pyzstd
import re
import sys
import pypipegraph2 as ppg2


def main_filter_constraint_violations(filename=None, pipegraph=None):
    """For when you really need to remove some jobs from the pipegraph's history.
    Takes a filename with job ids.
    Defaults to .ppg/errors/latest/constraint_violations.jobs

    If you have your ppg history in a non standard place, pass in a ppg
    with the correct directories set.
    """
    if "--help" in sys.argv:
        print(
            "ppg2-cli filter_constraint_violations [filename] - filter constraint violations from history"
        )
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


def main_interactive():
    """Interactive mode.
    Watch files (names passed on stdin or as arguments).
    On change (and startup), exec the contents,
    and call all functions starting with ppg_.
    These function (re)define ppg-jobs,
    and then a ppg is run.
    Retain state and other non-changed stops between invocations.


    Template:
    ```
    import pypipegraph2 as ppg
    if __name__ == '__main__':
        ppg.new()
    def ppg_jobs():
        return ppg.FileGeneratingJob(...)

    if __name__ == '__main__':
        for symbol in globals():
            if symbol.startswith("ppg_"):
                globals()[symbol]()
         ppg.run()
    ```
    """
    from watchfiles import watch, Change
    import rich.console
    import rich.traceback

    if len(sys.argv) > 1:
        files = sys.argv[1:]
    else:
        files = sys.stdin.read().strip().split("\n")
    files = [Path(x).absolute() for x in files]

    console = rich.console.Console()
    func_hashes = {}

    logger.remove()
    install_logger()

    ppg.new(
        run_mode=ppg.RunMode.CONSOLE_INTERACTIVE,
        dir_config=ppg.DirConfig(".ppg/" + Path(files[0]).name),
    )
    last_times = {}

    for changes in watch(".", recursive=False):
        for change in changes:
            # print(change)
            if change[0] in (Change.modified, Change.added):
                # print("changed file", change[1])
                if Path(change[1]).absolute() in files:
                    try:
                        find_changed_funcs(change[1])
                    except:
                        print("error running found changes in ", change[1])
                if Path(change[1]).absolute() == Path(__file__).absolute():
                    reload_iv()


def find_changed_funcs(filename):
    logger.info(f"Change in {filename}")
    try:
        code = Path(filename).read_text()
    except FileNotFoundError:
        time.sleep(0.1)
        code = Path(filename).read_text()
    l = {}
    compiled_code = compile(code, filename, "exec")
    exec(compiled_code, l, l) # globals = locals, just like in a regular eval.
    logger.info(f"Executed  {filename}")
    any_changed = False
    for name, obj in l.items():
        if name.startswith("ppg_") and callable(obj):
            path = filename + "::" + name
            dis = ppg.jobs._FunctionInvariant.get_dis(obj)
            closure = ppg.jobs._FunctionInvariant.extract_closure(obj)
            func_hash = (dis, closure)
            if func_hash != func_hashes.get(path, None):
                logger.warning(f"\t Change in {name}")
                func_hashes[path] = func_hash
                any_changed = True
                try:
                    obj()
                except Exception as e:
                    print("error evaluating", path)
                    console.print_exception(show_locals=True)
    if any_changed:
        logger.info("Run ppg")
        ppg.run()
        logger.info("Done ppg")
        install_logger()


def reload_iv():
    logger.error("reloading iv")
    python_path = sys.executable
    os.execv(python_path, [python_path] + sys.argv)
    # and end of session for this prgorme.


def install_logger():
    logger.add(
        sink=sys.stderr,
        format="<blue>{elapsed}s</blue> | <level>{level.icon}</level> <bold>|</bold><level>{message}</level>",
    )
