"""
The MIT License (MIT)

Copyright (c) 2012, Florian Finkernagel <finkernagel@imt.uni-marburg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import os
import subprocess
import sys
import pytest
import pypipegraph as ppg
from .shared import write, read, append, Dummy, assertRaises
from pathlib import Path


@pytest.mark.usefixtures("ppg1_compatibility_test")
class TestResourceCoordinator:
    def test_jobs_that_need_all_cores_are_spawned_one_by_one(
        self, ppg1_compatibility_test
    ):
        # we'll determine this by the start respective end times..
        import time

        ppg1_compatibility_test.new_pipegraph(
            quiet=True,
            resource_coordinator=ppg.resource_coordinators.LocalSystem(
                max_cores_to_use=3, interactive=False
            ),
            # ppg2: needs 3 cores.
            # Two cores, second job is at 2-1 = 1 core, can still run...
            dump_graph=False,
        )
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda: write("out/A", "A") and time.sleep(0.1)
        )
        jobB = ppg.FileGeneratingJob(
            "out/B", lambda: write("out/B", "B") and time.sleep(0.2)
        )
        jobA.cores_needed = -1
        jobB.cores_needed = -1
        ppg.run_pipegraph()
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
            "\n",
            first_job.start_time,
            "\n",
            first_job.stop_time,
            "\n",
            second_job.start_time,
            "\n",
            second_job.stop_time,
            "\n",
        )
        if jobA.start_time is None:
            raise ValueError("JobA did not run")
        assert first_job.stop_time < second_job.start_time

    def test_jobs_concurrent_jobs_run_concurrently(self, ppg1_compatibility_test):
        # we'll determine this by the start respective end times..
        import time

        ppg1_compatibility_test.new_pipegraph(
            resource_coordinator=ppg.resource_coordinators.LocalSystem(
                max_cores_to_use=2, interactive=False
            ),
            quiet=True,
            dump_graph=False,
        )
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda: time.sleep(0.5) or write("out/A", "A")
        )
        jobB = ppg.FileGeneratingJob(
            "out/B", lambda: time.sleep(0.5) or write("out/B", "B")
        )
        jobA.cores_needed = 1
        jobB.cores_needed = 1
        now = time.time()
        ppg.run_pipegraph()
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


class CantDepickle:
    """A class that can't be depickled (throws a type error,
    just like the numpy.maskedarray does occacionally)"""

    def __getstate__(self):
        return {"shu": "5"}

    def __setstate__(self, state):
        print(state)
        raise TypeError("I can be pickled, but not unpickled")


@pytest.mark.usefixtures("ppg1_compatibility_test")
class TestingTheUnexpectedTests:
    def test_job_killing_python(self):
        def dies():
            import sys

            # logging.info("Now terminating child python")
            sys.exit(5)

        fg = ppg.FileGeneratingJob("out/A", dies)
        try:
            # ppg.util.global_pipegraph.rc.timeout = 1
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert not (os.path.exists("out/A"))
        # assert isinstance(fg.exception, ppg.JobDiedException)
        # ppg2
        da_error = ppg.util.global_pipegraph.last_run_result[fg.job_id].error.args[0]
        assert isinstance(da_error, ppg.JobDiedException)
        assert da_error.exit_code == 5

    def test_job_killing_python_stdout_stderr_logged(self):
        def dies():
            import sys

            # logging.info("Now terminating child python")
            print("hello")
            sys.stderr.write("I am stderr\n")
            sys.stdout.flush()
            sys.exit(5)

        fg = ppg.FileGeneratingJob("out/A", dies)
        try:
            # ppg.util.global_pipegraph.rc.timeout = 1
            ppg.run_pipegraph()
            raise ValueError("should not be reached")
        except ppg.RuntimeError:
            pass
        assert not (os.path.exists("out/A"))
        # assert isinstance(fg.exception, ppg.JobDiedException)
        # ppg2
        da_error = ppg.util.global_pipegraph.last_run_result[fg.job_id].error.args[0]
        assert isinstance(da_error, ppg.JobDiedException)
        assert da_error.exit_code == 5
        assert fg.stdout == "hello\n"
        assert fg.stderr == "I am stderr\n"

    @pytest.mark.skip  # ParameterInvariant no longer uses pickle
    def test_unpickle_bug_prevents_single_job_from_unpickling(
        self, ppg1_compatibility_test
    ):
        def do_a():
            write("out/A", "A")
            append("out/As", "A")

        ppg.FileGeneratingJob("out/A", do_a)

        def do_b():
            write("out/B", "A")
            append("out/Bs", "A")

        job_B = ppg.FileGeneratingJob("out/B", do_b)
        cd = CantDepickle()
        job_parameter_unpickle_problem = ppg.ParameterInvariant("C", (cd,))
        job_B.depends_on(job_parameter_unpickle_problem)
        ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/As") == "A"
        assert read("out/B") == "A"
        assert read("out/Bs") == "A"
        print("second run")
        ppg1_compatibility_test.new_pipegraph(dump_graph=False)

        ppg.FileGeneratingJob("out/A", do_a)
        job_B = ppg.FileGeneratingJob("out/B", do_b)
        job_parameter_unpickle_problem = ppg.ParameterInvariant("C", (cd,))
        job_B.depends_on(job_parameter_unpickle_problem)
        with pytest.raises(ppg.RuntimeError):
            ppg.run_pipegraph()
        assert read("out/A") == "A"
        assert read("out/As") == "A"
        assert read("out/B") == "A"
        assert (
            read("out/Bs") == "AA"
        )  # this one got rerun because we could not load the invariant...

    @pytest.mark.skip  # tested by ppg2 test.
    def testing_import_does_not_hang(self):  # see python issue22853
        old_dir = os.getcwd()
        os.chdir(os.path.dirname(__file__))
        p = subprocess.Popen(
            [sys.executable, "_import_does_not_hang.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        print(stdout, stderr)
        assert b"OK" in stdout
        os.chdir(old_dir)

    def test_older_jobs_added_back_to_ppg1_compatibility_test(
        self, ppg1_compatibility_test
    ):
        a = ppg.FileGeneratingJob("out/A", lambda of: write(of, "a"))
        ppg.util.global_pipegraph.run()
        ppg1_compatibility_test.new_pipegraph()
        b = ppg.FileGeneratingJob("out/B", lambda of: write(of, "b"))
        with pytest.raises(KeyError):  # ppg2 ppg.PyPipeGraphError):
            a.depends_on(b)
        with pytest.raises(KeyError):  # ppg2 ppg.PyPipeGraphError):
            b.depends_on(a)


class TestsNotImplemented:
    @pytest.mark.xfail
    def test_temp_jobs_and_gen_jobs(self):
        # DependencyInjection A creates TempJob B and job C (c is already done)
        # DependencyInjeciton D (dep on A) creates TempJob B and job E
        # Now, When A runs, B is created, and not added to the jobs-to-run list
        # since it is not necessary (with C being done).
        # now D runs, B would not be required by E, but does not get added to the
        # run list (since it is not new), and later on, the sanity check crashes.

        # alternativly, if C is not done, execution order is A, B, C. Then cleanup
        # for B happens, then D is run, the E explodes, because cleanup has been done!

        # now, if A returns B, it get's injected into the dependenies of D,
        # the exeuction order is correct, but B get's done no matter what because D
        # now requires it, even if both C and E have already been done.

        # what a conundrum
        raise NotImplementedError()

    @pytest.mark.xfail
    def test_cached_job_done_but_gets_invalidated_by_dependency_injection_generated_job(
        self,
    ):
        # very similar to the previous case,
        # this basically directly get's you into the 'Job execution order territory...'
        raise NotImplementedError()


@pytest.mark.usefixtures("ppg1_compatibility_test")
class TestPathLib:
    def test_multifilegenerating_job_requires_string_filenames(self):
        import pathlib

        x = lambda: 5  # noqa:E731
        ppg.MultiFileGeneratingJob(["a"], x)
        ppg.MultiFileGeneratingJob([pathlib.Path("a")], x)

        def inner():
            ppg.MultiFileGeneratingJob([0])

        assertRaises(TypeError, inner)

        def inner():
            ppg.MultiFileGeneratingJob([b"a"])  # bytes is not a string type

        assertRaises(TypeError, inner)

    def test_accepts(self):
        import pathlib

        write("aaa", "hello")
        write("bbb", "hello")
        write("ccc", "hello")
        a = ppg.FileTimeInvariant(pathlib.Path("aaa"))
        a1 = ppg.MultiFileInvariant([pathlib.Path("bbb"), "ccc"])
        b = ppg.FileGeneratingJob(
            pathlib.Path("b"),
            lambda of: write(of, "bb" + read("aaa") + read("bbb") + read("ccc")),
        )
        b.depends_on(a)
        b.depends_on(a1)

        dd = Dummy()

        def mf():
            write("c", "cc" + read("g"))
            write("d", "dd" + read("h") + dd.attr)
            write("e", "ee" + read("i"))  # ppg2 + read("j"))

        c = ppg.MultiFileGeneratingJob([pathlib.Path("c"), "d", pathlib.Path("e")], mf)
        c.depends_on(b)
        d = ppg.FunctionInvariant(pathlib.Path("f"), lambda x: x + 1)
        c.depends_on(d)
        e = ppg.ParameterInvariant(pathlib.Path("c"), "hello")
        c.depends_on(e)
        f = ppg.TempFileGeneratingJob(pathlib.Path("g"), lambda: write("g", "gg"))
        c.depends_on(f)

        def tmf():
            write("h", "hh")
            write("i", "ii")

        g = ppg.MultiTempFileGeneratingJob([pathlib.Path("h"), "i"], tmf)
        c.depends_on(g)

        def tpf():
            write("j", "jjjj")
            write("k", "kkkk")

        # ppg2
        # h = ppg.TempFilePlusGeneratingJob(pathlib.Path("j"), pathlib.Path("k"), tpf)
        # c.depends_on(h)

        i = ppg.CachedDataLoadingJob(
            pathlib.Path("l"), lambda: write("l", "llll"), lambda res: res
        )
        c.depends_on(i)

        m = ppg.CachedAttributeLoadingJob(pathlib.Path("m"), dd, "attr", lambda: "55")
        c.depends_on(m)
        ppg.run_pipegraph()
        assert read("aaa") == "hello"
        assert read("b") == "bbhellohellohello"
        assert read("c") == "ccgg"
        assert read("d") == "ddhh55"
        assert read("e") == "eeii"  # ppg2 jjjj"
        assert not (os.path.exists("g"))
        assert not (os.path.exists("h"))
        assert not (os.path.exists("i"))
        # assert not (os.path.exists("j"))
        # assert read("k") == "kkkk"


def test_fixture_without_class(ppg1_compatibility_test):
    import pathlib

    assert "run/.test_fixture_without_class" in str(pathlib.Path(".").absolute())


def test_job_or_filename(ppg1_compatibility_test):
    a, dep_a = ppg.util.job_or_filename("out/A")
    assert a == Path("out/A")  # ppg2 now returns Path
    assert len(dep_a) == 1
    assert isinstance(dep_a[0], ppg.RobustFileChecksumInvariant)
    j = ppg.FileGeneratingJob("out/B", lambda: None)
    b, dep_b = ppg.util.job_or_filename(j)
    assert b == Path("out/B")
    assert dep_b[0] is j
    assert len(dep_b) == 1

    c, dep_c = ppg.util.job_or_filename(None)
    assert c is None
    assert not dep_c


@pytest.mark.skip  # ppg2 dos not have a stat caceh
def test_stat_cache(ppg1_compatibility_test):
    import time

    write("out/A", "A")
    assert ppg.util.stat("out/A")
    os.unlink("out/A")
    assert ppg.util.stat("out/A")  # cached...
    time.sleep(1)
    with pytest.raises(FileNotFoundError):
        ppg.util.stat("out/A")  # cache invalidated


@pytest.mark.skip  # ppg2 has it's own test suite
def test_interactive_import(ppg1_compatibility_test):
    # just so at least the import part of interactive is under coverage
    import pypipegraph.interactive  # noqa:F401


@pytest.mark.skip  # ppg2 changed this completely
def test_logging(ppg1_compatibility_test):
    import logging

    my_logger = logging.getLogger("pypipegraph")
    h = logging.FileHandler(filename="ppg.log", mode="w")
    my_logger.addHandler(h)
    logging.getLogger().warning("Should not be in the log.")
    try:
        my_logger.setLevel(logging.DEBUG)
        f = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        h.setFormatter(f)
        ppg.FileGeneratingJob("out/A", lambda: write("out/A", "A"))
        ppg.run_pipegraph()
    finally:
        my_logger.removeHandler(h)
    assert os.path.exists("ppg.log")
    d = read("ppg.log")
    assert not ("Should not be in the log.\n" in d)
    assert "pypipegraph - INFO" in d
    assert "pypipegraph - DEBUG" in d


@pytest.mark.skip  # handled by ppg2 tests
def test_version_is_correct():
    import configparser
    from pathlib import Path

    c = configparser.ConfigParser()
    c.read(Path(__file__).parent.parent / "setup.cfg")
    version = c["metadata"]["version"]
    assert version == ppg.__version__


def test_dataloading_job_changing_cwd(ppg1_compatibility_test):
    from pathlib import Path

    os.mkdir("shu")

    def load():
        os.chdir("shu")
        Path("b").write_text("world")
        return 55

    a = ppg.FileGeneratingJob("a", lambda: Path("a").write_text("hello"))
    b = ppg.DataLoadingJob("b", load)
    a.depends_on(b)
    # ppg2 no longer allows this, it's basically the thing it can't
    # recover from (multithreaded and cwd changes = boom
    with pytest.raises(ppg.RuntimeError):
        ppg.run_pipegraph()
    assert not Path("a").exists()
    assert Path("shu/b").exists()
    # assert read("a") == "hello"
    # assert read("shu/b") == "world"


def test_job_generating_job_changing_cwd(ppg1_compatibility_test):
    from pathlib import Path

    os.mkdir("shu")

    def load():
        os.chdir("shu")
        Path("b").write_text("world")
        return 55

    a = ppg.FileGeneratingJob("a", lambda: Path("a").write_text("hello"))
    b = ppg.JobGeneratingJob("b", load)
    a.depends_on(b)
    # ppg2 no longer allows this, it's basically the thing it can't recover
    # from (multithreaded and cwd changes = boom
    with pytest.raises(ppg.RuntimeError):
        ppg.run_pipegraph()
    assert not Path("a").exists()
    assert Path("shu/b").exists()
    # assert read("a") == "hello"
    # assert read("shu/b") == "world"


def test_inheritance_of_filegen(ppg1_compatibility_test, job_trace_log):
    class MyJob(ppg.FileGeneratingJob):
        def __init__(self, filename, func):
            def wrapper(of):
                append("counter", "a")
                func(of)

            super().__init__(filename, wrapper)

    MyJob("a", lambda of: of.write_text("hello"))
    ppg.run_pipegraph()
    assert read("a") == "hello"
    assert read("counter") == "a"


def test_util_checksum_file(ppg1_compatibility_test):
    import hashlib

    Path("a").write_text("hello world")
    should = hashlib.md5(b"hello world").hexdigest()
    assert ppg.util.checksum_file("a") == should


def test_depends_on_mfg_keeps_wrapping(ppg1_compatibility_test):
    a = ppg.MultiFileGeneratingJob(["a"], lambda ofs: 5)
    b = ppg.FileGeneratingJob("b", lambda of: 5)
    assert a.depends_on(b) is a


def test_cores_available(ppg1_compatibility_test):
    assert ppg.util.global_pipegraph.rc.cores_available > 0


def test_job_dot_failed(ppg1_compatibility_test):
    a = ppg.FileGeneratingJob("a", lambda of: of.write_text("a"))
    b = ppg.FileGeneratingJob("b", lambda of: None)
    with pytest.raises(ppg.RuntimeError):
        ppg.run_pipegraph()
    assert b.failed
    assert not a.failed
