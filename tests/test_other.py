import os
import time
import subprocess
import sys
import pytest
import pypipegraph2 as ppg
from pathlib import Path
from .shared import write, read, Dummy, counter


@pytest.mark.usefixtures("ppg2_per_test", "create_out_dir")
class TestResourceCoordinator:
    def test_jobs_that_need_all_cores_are_spawned_one_by_one(self):
        # we'll determine this by the start respective end times..
        ppg.new(
            cores=3,
        )

        def a(of):
            write(of, "A")
            time.sleep(0.1)

        def b(of):
            write(of, "B")
            time.sleep(0.1)

        jobA = ppg.FileGeneratingJob("out/A", a, resources=ppg.Resources.AllCores)
        jobB = ppg.FileGeneratingJob("out/B", b, resources=ppg.Resources.AllCores)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        if jobA.start_time < jobB.start_time:
            first_job = jobA
            second_job = jobB
        else:
            first_job = jobB
            second_job = jobA
        min_start = min(jobA.start_time, jobB.start_time)
        print(
            "times",
            first_job.start_time - min_start,
            first_job.stop_time - min_start,
            second_job.start_time - min_start,
            second_job.stop_time - min_start,
        )
        if jobA.start_time is None:
            raise ValueError("JobA did not run")
        assert first_job.stop_time < second_job.start_time

    def test_jobs_concurrent_jobs_run_concurrently(self):
        # we'll determine this by the start respective end times..
        import time

        ppg.new(
            cores=2,
        )
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda of: write(of, "A"), resources=ppg.Resources.AllCores
        )
        jobB = ppg.FileGeneratingJob(
            "out/B", lambda of: write(of, "B"), resources=ppg.Resources.AllCores
        )
        now = time.time()
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        if jobA.start_time < jobB.start_time:
            first_job = jobA
            second_job = jobB
        else:
            first_job = jobB
            second_job = jobA
        print(
            "times",
            first_job.start_time - now,
            first_job.stop_time - now,
            second_job.start_time - now,
            second_job.stop_time - now,
        )
        if jobA.start_time is None:
            raise ValueError("JobA did not run")
        assert first_job.stop_time - now > second_job.start_time - now

    def test_multiple_all_cores_blocking_single_jobs(self):
        pass  # todo


class CantDepickle:
    """A class that can't be depickled (throws a type error,
    just like the numpy.maskedarray does occacionally)"""

    def __getstate__(self):
        return {"shu": "5"}

    def __setstate__(self, state):
        print(state)
        raise TypeError("I can be pickled, but not unpickled")


@pytest.mark.usefixtures("ppg2_per_test")
class TestingTheUnexpectedTests:
    def test_job_exiting_python(self):
        def dies(of):
            import sys
            from loguru import logger

            logger.info("Now terminating child python")
            sys.exit(5)

        ppg.FileGeneratingJob("out/A", dies)
        with pytest.raises(ppg.JobsFailed):
            # ppg.util.global_pipegraph.rc.timeout = 1
            ppg.run()
        assert not (Path("out/A").exists())
        error = ppg.global_pipegraph.last_run_result["out/A"].error
        assert isinstance(error.args[0], ppg.JobDied)
        assert error.args[0].args[2] == 5

    def test_job_exiting_python_stdout_stderr_logged(self):
        def dies(of):
            import sys

            # logging.info("Now terminating child python")
            print("hello")
            sys.stderr.write("I am stderr\n")
            sys.stdout.flush()
            sys.exit(5)

        fg = ppg.FileGeneratingJob("out/A", dies)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
            ppg.run()
        assert not (Path("out/A").exists())
        error = ppg.global_pipegraph.last_run_result["out/A"].error
        assert isinstance(error.args[0], ppg.JobDied)
        assert error.args[0].args[2] == 5
        assert fg.stdout == "hello\n"
        assert fg.stderr == "I am stderr\n"

    def test_job_getting_killed_python_stdout_stderr_logged(self):
        def dies(of):
            import sys
            import signal

            # logging.info("Now terminating child python")
            print("hello")
            sys.stderr.write("I am stderr\n")
            sys.stdout.flush()
            sys.stderr.flush()
            os.kill(os.getpid(), signal.SIGKILL)

        fg = ppg.FileGeneratingJob("out/A", dies)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
            ppg.run()
        assert not (Path("out/A").exists())
        error = ppg.global_pipegraph.last_run_result["out/A"].error
        assert isinstance(error.args[0], ppg.JobDied)
        assert error.args[0].args[2] == -9
        assert fg.stdout == "hello\n"
        assert fg.stderr == "I am stderr\n"

    def testing_import_does_not_hang(self):  # see python issue22853
        old_dir = os.getcwd()
        os.chdir(Path(__file__).parent)
        p = subprocess.Popen(
            [sys.executable, "_import_does_not_hang.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        print(stdout, stderr)
        assert b"OK" in stdout
        os.chdir(old_dir)

    def test_older_jobs_added_back_to_new_pipegraph(self, create_out_dir):
        a = ppg.FileGeneratingJob("out/A", lambda of: write(of, "a"))
        # ppg.run()
        ppg.new()
        b = ppg.FileGeneratingJob("out/B", lambda of: write(of, "b"))
        with pytest.raises(KeyError):
            a.depends_on(b)
        with pytest.raises(KeyError):
            b.depends_on(a)
        a.readd()
        a.depends_on(b)
        assert "out/B" in ppg.global_pipegraph.jobs
        assert "out/A" in ppg.global_pipegraph.jobs
        # with pytest.raises(ppg.PyPipeGraphError):


@pytest.mark.usefixtures("ppg2_per_test")
class TestPathLib:
    def test_multifilegenerating_job_requires_string_filenames(self):
        import pathlib

        x = lambda of: 5  # noqa:E731
        ppg.MultiFileGeneratingJob(["a"], x)
        ppg.MultiFileGeneratingJob([pathlib.Path("a")], x)

        with pytest.raises(TypeError):
            ppg.MultiFileGeneratingJob([0])

        with pytest.raises(TypeError):
            ppg.MultiFileGeneratingJob([b"a"])  # bytes is not a string type

    def test_accepts(self):
        import pathlib

        write("aaa", "hello")
        write("bbb", "hello")
        write("ccc", "hello")
        a = ppg.FileInvariant(pathlib.Path("aaa"))
        b = ppg.FileGeneratingJob(
            pathlib.Path("b"),
            lambda of: write(of, "bb" + read("aaa") + read("bbb") + read("ccc")),
        )
        b.depends_on(a)

        dd = Dummy()

        def mf(ofs):
            ofs[0].write_text("cc" + read("g"))
            ofs[1].write_text("dd" + read("h") + dd.attr)
            ofs[2].write_text("ee" + read("i") + read("j") + read("k"))

        c = ppg.MultiFileGeneratingJob([pathlib.Path("c"), "d", pathlib.Path("e")], mf)
        c.depends_on(b)
        d = ppg.FunctionInvariant(pathlib.Path("f"), lambda x: x + 1)
        c.depends_on(d)
        e = ppg.ParameterInvariant(pathlib.Path("c"), "hello")
        c.depends_on(e)
        f = ppg.TempFileGeneratingJob(pathlib.Path("g"), lambda of: write(of, "gg"))
        c.depends_on(f)

        def tmf(ofs):
            ofs[0].write_text("hh")
            ofs[1].write_text("ii")

        g = ppg.MultiTempFileGeneratingJob([pathlib.Path("h"), "i"], tmf)
        c.depends_on(g)

        def tpf(ofs):
            assert len(ofs) == 2
            write(ofs[0], "jjjj")
            write(ofs[1], "kkkk")

        with pytest.raises(ValueError):
            h = ppg.MultiTempFileGeneratingJob(
                [pathlib.Path("j"), "k", pathlib.Path("k")], tpf
            )
        h = ppg.MultiTempFileGeneratingJob([pathlib.Path("j"), "k"], tpf)
        c.depends_on(h)

        i = ppg.CachedDataLoadingJob(
            pathlib.Path("l"), lambda: write("l", "llll"), lambda res: res
        )
        with pytest.raises(TypeError):
            c.depends_on(i)
        c.depends_on(i.load)

        m = ppg.CachedAttributeLoadingJob(pathlib.Path("m"), dd, "attr", lambda: "55")
        c.depends_on(m.load)
        ppg.run()
        assert read("aaa") == "hello"
        assert read("b") == "bbhellohellohello"
        assert read("c") == "ccgg"
        assert read("d") == "ddhh55"
        assert read("e") == "eeiijjjjkkkk"
        assert not (Path("g").exists())
        assert not (Path("h").exists())
        assert not (Path("i").exists())
        assert not (Path("j").exists())


def test_fixture_without_class(ppg2_per_test):
    # just to make sure the ppg2_per_test fixture does what it's supposed to if you're not using a class
    import pathlib

    assert "run/.test_fixture_without_class" in str(pathlib.Path(".").absolute())


def test_job_or_filename(ppg2_per_test):
    a, dep_a = ppg.util.job_or_filename("out/A")
    assert a == Path("out/A")
    assert len(dep_a) == 1
    assert isinstance(dep_a[0], ppg.FileInvariant)
    j = ppg.FileGeneratingJob("out/B", lambda of: None)
    b, dep_b = ppg.util.job_or_filename(j)
    assert b == Path("out/B")
    assert dep_b[0] is j
    assert len(dep_b) == 1

    c, dep_c = ppg.util.job_or_filename(None)
    assert c is None
    assert not dep_c

    b2, dep_b2 = ppg.util.job_or_filename("out/B")
    assert b2 == ("out/B")
    assert dep_b2 == ["out/B"]  # still not a job, but it's findable downstream


@pytest.mark.xfail()  # todo
def test_interactive_import(ppg2_per_test):
    # just so at least the import part of interactive is under coverage
    import pypipegraph2.interactive  # noqa:F401


def test_dataloading_job_changing_cwd(ppg2_per_test):
    from pathlib import Path

    os.mkdir("shu")

    def load():
        os.chdir("shu")
        Path("b").write_text("world")
        return 55

    a = ppg.FileGeneratingJob("a", lambda of: of.write_text("hello"))
    b = ppg.DataLoadingJob("b", load)
    a.depends_on(b)
    with pytest.raises(ppg.JobsFailed):
        ppg.run()
    assert isinstance(
        ppg.global_pipegraph.last_run_result["b"].error.args[0], ppg.JobContractError
    )


def test_job_generating_job_changing_cwd(ppg2_per_test):
    from pathlib import Path

    os.mkdir("shu")

    def load():
        os.chdir("shu")
        Path("b").write_text("world")
        return 55

    a = ppg.FileGeneratingJob("a", lambda of: Path("a").write_text("hello"))
    b = ppg.JobGeneratingJob("b", load)
    a.depends_on(b)
    with pytest.raises(ppg.JobsFailed):
        ppg.run()
    assert isinstance(
        ppg.global_pipegraph.last_run_result["b"].error.args[0], ppg.JobContractError
    )


def test_capturing_locals_when_they_have_throwing_str(ppg2_per_test):
    class NoStr:
        def __str__(self):
            raise ValueError("Cant string this")

    def inner(of):
        a = NoStr()
        raise ValueError("expected")  # trace check

    j = ppg.FileGeneratingJob("a", inner)
    with pytest.raises(ppg.JobsFailed):
        ppg.run()
    assert "expected" in str(j.exception)
    assert "trace check" in str(j.stack_trace)  # we captured teh relevant line


class TestCleanup:
    def test_error_cleanup(self, ppg2_per_test):
        import pypipegraph2 as ppg2

        ppg2_per_test.new(log_retention=2)
        assert ppg2.global_pipegraph.log_retention == 2

        def fail(of):
            raise ValueError()

        job = ppg.FileGeneratingJob("A", fail)
        ec = []  # counters
        lc = []
        with pytest.raises(ppg.JobsFailed):
            ppg.run(print_failures=False)
        ec.append(len(list(ppg.global_pipegraph.dir_config.error_dir.glob("*"))))
        lc.append(len(list(ppg.global_pipegraph.dir_config.log_dir.glob("*.log"))))
        with pytest.raises(ppg.JobsFailed):
            ppg.run(print_failures=False)
        ec.append(len(list(ppg.global_pipegraph.dir_config.error_dir.glob("*"))))
        lc.append(len(list(ppg.global_pipegraph.dir_config.log_dir.glob("*.log"))))
        with pytest.raises(ppg.JobsFailed):
            ppg.run(print_failures=False)
        # we keep  log_retention old ones + the current one
        ec.append(len(list(ppg.global_pipegraph.dir_config.error_dir.glob("*"))))
        lc.append(len(list(ppg.global_pipegraph.dir_config.log_dir.glob("*.log"))))

        with pytest.raises(ppg.JobsFailed):
            ppg.run(print_failures=False)
        assert ppg.global_pipegraph.log_file.exists()
        ec.append(len(list(ppg.global_pipegraph.dir_config.error_dir.glob("*"))))
        lc.append(len(list(ppg.global_pipegraph.dir_config.log_dir.glob("*.log"))))
        assert ec == [
            1 + 1,
            2 + 1,
            3 + 1,  # latest
            3 + 1,
        ]
        assert lc == [
            1 * 2,  # includes messages.log and lookup.log
            2 * 2,
            3 * 2,  # latest exluded by *.log
            3 * 2,
        ]


def test_exploding_core_lock_captured(ppg2_per_test):
    """We had an issue where an exception in acquiring the core lock led to the thread dying
    and the graph runner hanging
    """
    a = ppg.FileGeneratingJob(
        "a", lambda of: of.write_text("a"), resources=ppg.Resources._RaiseInCoreLock
    )
    with pytest.raises(ppg.FatalGraphException):
        ppg.run()
    assert "Count == 0" in str(a.exception)


def test_exploding_resources_to_number(ppg2_per_test):
    """We had an issue where an exception in acquiring the core lock led to the thread dying
    and the graph runner hanging
    """
    a = ppg.FileGeneratingJob(
        "a", lambda of: of.write_text("a"), resources=ppg.Resources._RaiseInToNumber
    )
    with pytest.raises(ppg.FatalGraphException):
        ppg.run()
    assert "Not a Resource" in str(a.exception)


@pytest.mark.usefixtures("ppg2_per_test")
class TestModifyDag:
    def test_2_fg_one_dl(self):
        from pypipegraph2.util import log_info

        parts = []
        a1 = ppg.FileGeneratingJob(
            "a1", lambda of: of.write_text(str(parts)), depend_on_function=False
        )
        a2 = ppg.FileGeneratingJob(
            "a2",
            lambda of: of.write_text(Path("a1").read_text() + str(parts)),
            depend_on_function=False,
        )
        a2.depends_on(a1)
        b = ppg.DataLoadingJob(
            "b", lambda *args: parts.append(1) or ppg.UseInputHashesForOutput(), depend_on_function=False
        )
        a1.depends_on(b)
        a2.depends_on(b)
        log_info("now run")
        ppg.run()
        assert read("a1") == "[1]"
        assert read("a2") == "[1][1]"

    def test_2_fg_one_dl_failing(pself, ppg2_per_test, job_trace_log):
        from pypipegraph2.util import log_info

        parts = []
        a1 = ppg.FileGeneratingJob(
            "a1", lambda of: of.write_text("a1"), depend_on_function=False
        )
        a2 = ppg.FileGeneratingJob(
            "a2",
            lambda of: of.write_text(Path("a2").read_text() + str(parts)),
            depend_on_function=False,
        )
        a2.depends_on(a1)
        b = ppg.DataLoadingJob(
            "b", lambda: _no_such_thing, depend_on_function=False  # noqa: F821
        )
        a1.depends_on(b)
        a2.depends_on(b)
        log_info("now run")
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert type(b.exception) is NameError
        assert "Upstream" in str(a1.exception)
        assert "Upstream" in str(a2.exception)
        assert not Path("a1").exists()
        assert not Path("a2").exists()

    def test_2_fg_one_temp(ppg2_per_test):
        from pypipegraph2.util import log_info

        parts = []
        a1 = ppg.FileGeneratingJob(
            "a1",
            lambda of: of.write_text(Path("b").read_text() + "a1"),
            depend_on_function=False,
        )
        a2 = ppg.FileGeneratingJob(
            "a2",
            lambda of: of.write_text(
                Path("b").read_text() + Path("a1").read_text() + "a2"
            ),
            depend_on_function=False,
        )
        a2.depends_on(a1)
        b = ppg.TempFileGeneratingJob(
            "b", lambda of: of.write_text("b"), depend_on_function=False
        )
        a1.depends_on(b)
        a2.depends_on(b)

        log_info("now run")
        ppg.run()
        assert read("a1") == "ba1"
        assert read("a2") == "bba1a2"
        assert not Path("b").exists()


def test_prevent_absolute_paths(ppg2_per_test):
    ppg2_per_test.new(prevent_absolute_paths=True)
    with pytest.raises(ValueError):
        ppg.FileGeneratingJob("/tmp/absolute", lambda of: of.write_text("a"))
    ppg2_per_test.new(prevent_absolute_paths=False)
    ppg.FileGeneratingJob("/tmp/absolute", lambda of: of.write_text("a"))
    ppg2_per_test.new(prevent_absolute_paths=True)


def test_broken_case_from_delayeddataframe(ppg2_per_test):
    out = {}

    def store(key, value):
        out[key] = value
        return ppg.UseInputHashesForOutput()

    a = ppg.CachedDataLoadingJob(
        "a", lambda: "a", lambda value: counter("A") and store("a", value)
    )
    event = ppg.CachedDataLoadingJob(
        "event",
        lambda: counter("EVENT") and "event",
        lambda value: store("event", value),
    )
    event2 = ppg.CachedDataLoadingJob(
        "event2",
        lambda: counter("EVENT2") and "event2",
        lambda value: store("event2", value),
    )
    anno_sequence = ppg.CachedDataLoadingJob(
        "anno_sequence",
        lambda: counter("ANNO_SEQUENCE") and "anno_sequence",
        lambda value: store("anno_sequence", value),
    )
    event_seq = ppg.DataLoadingJob(
        "event_seq", lambda: store("event_seq", out["event"] + out["anno_sequence"])
    )
    event2_seq = ppg.DataLoadingJob(
        "event2_seq", lambda: store("event2_seq", out["event2"] + out["event_seq"])
    )
    force_load = ppg.JobGeneratingJob("force_load", lambda: None)

    anno_sequence.calc.depends_on(a.load)
    anno_sequence.load.depends_on(a.load)
    event.calc.depends_on(a.load)

    event_seq.depends_on(event.load)
    event_seq.depends_on(anno_sequence.load)

    event2.calc.depends_on(event.load)

    event2_seq.depends_on(event_seq, event2.load)

    force_load.depends_on(event.load, event2.load, event2_seq, event_seq)
    ppg.run()
    assert out["event2_seq"] == "event2" + "event" + "anno_sequence"
    assert read("EVENT") == "1"
    assert read("EVENT2") == "1"
    assert read("ANNO_SEQUENCE") == "1"
    ppg.run()
    assert read("EVENT") == "1"
    assert read("EVENT2") == "1"
    assert read("ANNO_SEQUENCE") == "1"


def test_strict_mode_two_jobs_same_id(ppg2_per_test):
    ppg.new(run_mode=ppg.RunMode.CONSOLE)
    assert ppg.global_pipegraph.run_mode.is_strict()
    a = lambda of: None  # noqa: E731
    ppg.FileGeneratingJob("a", a, depend_on_function=False)
    ppg.FileGeneratingJob("a", a, depend_on_function=False)
    # the testing is actually on the FunctionInvariant :(
    ppg.FileGeneratingJob("a", a, depend_on_function=True)
    with pytest.raises(ppg.JobRedefinitionError):
        ppg.FileGeneratingJob(
            "a", lambda of: of.write_text("h"), depend_on_function=True
        )


def test_calling_function_difference():
    def gen(x):
        return lambda: x + 5

    a = gen(5)
    b = gen(10)
    r = ppg.FunctionInvariant.debug_function_differences(a, b)
    assert "The function closures differed." in r
    assert (
        ppg.FunctionInvariant.debug_function_differences(None, None) == "No difference"
    )
    assert (
        ppg.FunctionInvariant.debug_function_differences(gen, gen)
        == "The functions were identical"
    )


def test_spawned_processes_get_killed_on_abort(ppg2_per_test, job_trace_log):
    import psutil

    ppg.new(cores=2)

    # this process doesn't get reaped
    p = subprocess.Popen("cat", stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    assert p.pid

    def long_running_proc():
        p = subprocess.Popen(["sleep", "5"], stdin=subprocess.PIPE)
        Path("pid").write_text(str(p.pid))
        ppg.global_pipegraph.runner.abort()
        p.communicate()
        Path("done").write_text("done")

    a = ppg.DataLoadingJob("a", long_running_proc)
    b = ppg.FileGeneratingJob("b", lambda of: of.write_text(str(of))).depends_on(a)
    with pytest.raises(ppg.JobsFailed):
        ppg.run()
    assert not Path("done").exists()
    time.sleep(1)
    assert psutil.pid_exists(p.pid)
    pid = int(Path("pid").read_text())
    ok = False
    try:
        ok = psutil.Process(pid).status() == "zombie"
    except psutil.NoSuchProcess:
        ok = True
    if not ok:
        # subprocess.check_call(["ps", "xf", "-o", "pid,ppid,pgid,cmd"])
        raise ValueError("Process was neither zombie (since parent is dead), nor gone.")

    p.communicate()


def test_spawned_processes_get_killed_on_catastrophic_process_failure(
    ppg2_per_test, job_trace_log
):
    import psutil

    path = Path(__file__).parent / "test_parent_termination_kills_children"
    p = subprocess.Popen([sys.executable, path / "stage2.py"], env=os.environ)
    p.communicate()

    for proc in psutil.process_iter(["cmdline"]):
        cmdline = proc.info["cmdline"] or ""
        if "ppg2_test_parallel_sentinel" in cmdline:
            raise ValueError(
                "children not killed, found a ppg2_test_parallel_sentinel",
                "pid",
                proc.pid,
                "ppid",
                proc.ppid(),
            )
        if "sleep" in cmdline and "5.51234" in cmdline:
            raise ValueError(
                "children not killed, found a sleep",
                "pid",
                proc.pid,
                "ppid",
                proc.ppid(),
            )


def test_tempmultifilegen_changing_output_but_had_been_validated(ppg2_per_test):
    import random

    def bad(files):
        files[0].write_text("hello")
        files[1].write_text("world" + str(random.random()))

    tmfg = ppg.MultiTempFileGeneratingJob(["a", "b"], bad)
    c = ppg.FileGeneratingJob("c", lambda of: of.write_text(Path("a").read_text()))
    c.depends_on("a")
    ppg.run()
    ppg.new()
    tmfg = ppg.MultiTempFileGeneratingJob(["a", "b"], bad)
    c = ppg.FileGeneratingJob("c", lambda of: of.write_text(Path("a").read_text()))
    c.depends_on("a")
    d = ppg.FileGeneratingJob("d", lambda of: of.write_text(Path("a").read_text()))
    d.depends_on("a")
    with pytest.raises(ppg.JobsFailed):
        ppg.run()
    error = ppg.global_pipegraph.last_run_result[tmfg.job_id].error
    assert "changed output" in error and "ephemeral" in error.lower()


@pytest.mark.usefixtures("ppg2_per_test", "create_out_dir")
class TestHashersWithPremadeSha256:
    def test_reads_dot_sha256(self):
        a = Path("a.txt")
        asha256 = Path("a.txt.sha256")
        a.write_text("hello")
        asha256.write_text("a" * 64)
        h = ppg.hashers.hash_file(a)
        assert h['hash'] == 'a' * 64
        mtime = a.stat().st_mtime
        # now push sha256 into the past
        os.utime(asha256, (mtime - 100, mtime - 100))
        with pytest.raises(ppg.JobContractError):
            ppg.hashers.hash_file(a)
        asha256.unlink()
        h = ppg.hashers.hash_file(a)
        assert h['hash'] != 'abc'

        # or have a non hash in there.
        asha256.write_text("a")
        with pytest.raises(ppg.JobContractError):
            ppg.hashers.hash_file(a)
        asha256.write_text("a" * 63 + 'X')
        with pytest.raises(ppg.JobContractError):
            ppg.hashers.hash_file(a)
