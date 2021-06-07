import os
import time
import subprocess
import sys
import pytest
import pypipegraph2 as ppg
from pathlib import Path
from .shared import write, read, Dummy


@pytest.mark.usefixtures("ppg2_per_test", "create_out_dir")
class TestResourceCoordinator:
    def test_jobs_that_need_all_cores_are_spawned_one_by_one(self, job_trace_log):
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
        ppg.new(
            cores=2,
        )
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda of: write(of, "A"), resources=ppg.Resources.AllCores
        )
        jobB = ppg.FileGeneratingJob(
            "out/B", lambda of: write(of, "B"), resources=ppg.Resources.AllCores
        )
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
            first_job.start_time,
            first_job.stop_time,
            second_job.start_time,
            second_job.stop_time,
        )
        if jobA.start_time is None:
            raise ValueError("JobA did not run")
        assert first_job.stop_time > second_job.start_time

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
        with pytest.raises(ppg.RunFailed):
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
        with pytest.raises(ppg.RunFailed):
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
        with pytest.raises(ppg.RunFailed):
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

    def test_accepts(self, job_trace_log):
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


@pytest.mark.xfail()  # todo
def test_interactive_import(ppg2_per_test):
    # just so at least the import part of interactive is under coverage
    import pypipegraph2.interactive  # noqa:F401


def test_version_is_correct():
    import configparser
    from pathlib import Path

    c = configparser.ConfigParser()
    c.read(Path(__file__).parent.parent / "setup.cfg")
    version = c["metadata"]["version"]
    assert version == ppg.__version__


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
    with pytest.raises(ppg.RunFailed):
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
    with pytest.raises(ppg.RunFailed):
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
        raise ValueError("expected") # trace check
    j = ppg.FileGeneratingJob('a', inner)
    with pytest.raises(ppg.RunFailed):
        ppg.run()
    assert 'expected' in str(j.exception)
    assert 'trace check' in str(j.stack_trace)  # we captured teh relevant line


def test_cache_dir(ppg2_per_test):
    ppg.new(cache_dir = 'shu')
    assert Path('shu').exists()
    ppg.new(cache_dir = None)
    a = ppg.FileGeneratingJob('a', lambda of: of.write_text("A"))
    ppg.run()
    assert Path('a').read_text() == 'A'
