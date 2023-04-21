import sys
from loguru import logger  # noqa:F401

sys.path.append("../../python")
import pypipegraph2 as ppg  # noqa:E402
from pathlib import Path  # noqa:E402
import shutil  # noqa:E402


whitelist = ["simplest.py"]
print("\n" * 10)

for fn in Path(".").glob("*"):
    if not fn.name in whitelist:
        if fn.is_dir():
            shutil.rmtree(fn)
        else:
            fn.unlink()

ppg.enable_rust_logging()

ppg.new(log_level=1, cores=2)
ppg.util.do_jobtrace_log = True


class Dummy(object):
    pass


def write(filename, text):
    Path(filename).write_text(text)


def append(filename, text):
    p = Path(filename)
    if p.exists():
        old = p.read_text()
    else:
        old = ""
    p.write_text(old + text)


def writeappend(filename_write, filename_append, string):
    write(filename_write, string)
    append(filename_append, string)


def read(filename):
    return Path(filename).read_text()


def counter(filename):
    """Helper for counting invocations in a side-effect file"""
    try:
        res = int(Path(filename).read_text())
    except:  # noqa: E722
        res = 0
    Path(filename).write_text(str(res + 1))
    return str(res)


def force_load(job):
    """Force the loading of a Dataloading job that has no other dependents"""
    import pypipegraph2 as ppg

    ppg.JobGeneratingJob(job.job_id + "_gen", lambda: None).depends_on(job)


def simplest():
    ppg.FileGeneratingJob("deleteme", lambda of: of.write_text("hello"))


def cached_dl():
    Path("out").mkdir(exist_ok=True)
    o = Dummy()

    def calc():
        return ", ".join(str(x) for x in range(0, 100))

    def store(value):
        o.a = value

    job, cache_job = ppg.CachedDataLoadingJob("out/mycalc", calc, store)
    of = "out/A"

    def do_write(of):
        write(of, o.a)

    ppg.FileGeneratingJob(of, do_write).depends_on(job)
    ppg.run()
    assert read(of) == ", ".join(str(x) for x in range(0, 100))


def test_tempfile():
    jobA = ppg.TempFileGeneratingJob(
        "TA",
        lambda of: of.write_text("A" + counter("a")),
        depend_on_function=False,
    )
    jobB = ppg.FileGeneratingJob(
        "B",
        lambda of: of.write_text("B" + counter("c") + Path("TA").read_text()),
        depend_on_function=False,
    )
    jobB.depends_on(jobA)
    ppg.run()
    assert not Path("TA").exists()
    assert Path("B").exists()
    assert Path("B").read_text() == "B0A0"
    logger.error("Second run - no rerun")
    ppg.run()
    assert not Path("TA").exists()
    assert Path("B").exists()
    assert Path("B").read_text() == "B0A0"

    Path("B").unlink()
    logger.error("Third run - B output missing")
    ppg.run()
    assert not Path("TA").exists()
    assert Path("B").exists()
    assert Path("B").read_text() == "B1A1"


def test_just_chained_tempfile(self, trace_log):
    jobA = ppg.TempFileGeneratingJob("TA", lambda of: of.write_text("A" + counter("a")))
    jobB = ppg.TempFileGeneratingJob(
        "B", lambda of: of.write_text("B" + counter("b") + Path("TA").read_text())
    )
    jobB.depends_on(jobA)
    ppg.run()
    assert not Path("TA").exists()
    assert not Path(
        "a"
    ).exists()  # changed with the smarter hull stuff - they don't run for sideeffects
    assert not Path("B").exists()
    assert not Path("b").exists()


def test_changing_inputs_when_job_was_temporarily_missing(self):
    jobA = ppg.FileGeneratingJob("A", lambda of: counter("a") and of.write_text("AAA"))
    jobB = ppg.FileGeneratingJob(
        "B", lambda of: of.write_text("BBB" + Path("A").read_text())
    )
    jobB.depends_on(jobA)
    ppg.run()
    assert Path("A").read_text() == "AAA"
    assert Path("B").read_text() == "BBBAAA"
    assert Path("a").read_text() == "1"
    ppg.new()
    assert not "A" in ppg.global_pipegraph.jobs
    assert not "B" in ppg.global_pipegraph.jobs
    jobA = ppg.FileGeneratingJob("A", lambda of: counter("a") and of.write_text("AAAA"))
    assert not "B" in ppg.global_pipegraph.jobs
    ppg.run()
    assert Path("A").read_text() == "AAAA"
    assert Path("B").read_text() == "BBBAAA"  # not rerun
    assert Path("a").read_text() == "2"
    ppg.new()
    jobA = ppg.FileGeneratingJob("A", lambda of: counter("a") and of.write_text("AAAA"))
    jobB = ppg.FileGeneratingJob(
        "B", lambda of: of.write_text("BBB" + Path("A").read_text())
    )
    jobB.depends_on(jobA)
    ppg.run()
    assert Path("a").read_text() == "2"
    assert Path("B").read_text() == "BBBAAAA"


# test_changing_inputs_when_job_was_temporarily_missing(None)


def test_tempfile_chained_invalidate_leaf(self):
    ppg.new(cores=1, log_level=6)
    jobA = ppg.TempFileGeneratingJob(
        "TA", lambda of: of.write_text("A" + counter("a")), depend_on_function=False
    )
    jobB = ppg.TempFileGeneratingJob(
        "TB",
        lambda of: of.write_text("B" + counter("b") + Path("TA").read_text()),
        depend_on_function=False,
    )
    jobC = ppg.FileGeneratingJob(
        "C",
        lambda of: of.write_text("C" + counter("c") + Path("TB").read_text()),
        depend_on_function=False,
    )
    jobC.depends_on(jobB)
    jobB.depends_on(jobA)
    ppg.util.log_error("First run")
    ppg.run()
    assert not Path("TA").exists()
    assert not Path("TB").exists()
    assert Path("C").read_text() == "C0B0A0"
    logger.error("Second No op run.")
    ppg.run()
    assert Path("C").read_text() == "C0B0A0"
    assert not Path("TA").exists()
    assert not Path("TB").exists()

    jobC.depends_on(ppg.FunctionInvariant(lambda: 53, "lambda_52"))
    ppg.util.log_error("Third run - rerun because of FI")
    ppg.run()
    assert Path("C").read_text() == "C1B1A1"
    assert not Path("TA").exists()
    assert not Path("TB").exists()


test_tempfile_chained_invalidate_leaf(None)

ppg.run()
