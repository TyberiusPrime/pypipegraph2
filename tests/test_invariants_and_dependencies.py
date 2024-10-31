import os
import gzip
from pathlib import Path
from loguru import logger
import stat
import time
import hashlib
import shutil
import pytest
import pypipegraph2 as ppg
from .shared import write, read, append, Dummy, counter, force_load


class Undepickable(object):
    def __getstate__(self):
        return {"shu": 123}  # must not return falsey value

    def __setstate__(self, state):
        self.sha = state["shu"]
        import pickle

        raise pickle.UnpicklingError("SHU")


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg2_per_test")
class TestInvariant:
    def sentinel_count(self):
        sentinel = "out/sentinel"
        try:
            op = open(sentinel, "r")
            count = int(op.read())
            op.close()
        except IOError:
            count = 1
        op = open(sentinel, "w")
        op.write("%i" % (count + 1))
        op.close()
        return count

    def test_filegen_jobs_detect_code_change(self):
        of = "out/a"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        ppg.FileGeneratingJob(of, do_write)
        ppg.run()
        assert read(of) == "shu"
        ppg.new()
        ppg.FileGeneratingJob(of, do_write)
        ppg.run()
        assert read(of) == "shu"  # has not been run again...

        def do_write2(of):
            append(of, "sha")

        ppg.new()
        ppg.FileGeneratingJob(of, do_write2)
        ppg.run()
        assert read(of) == "sha"  # has been run again ;).

    def test_filegen_jobs_ignores_code_change(self):
        of = "out/a"

        def do_write(of):
            counter("A")
            append(of, "shu" * self.sentinel_count())

        ppg.FileGeneratingJob(of, do_write)
        ppg.run()

        assert read(of) == "shu"
        assert read("A") == "1"
        ppg.new()
        ppg.FileGeneratingJob(of, do_write)
        ppg.run()
        assert read(of) == "shu"  # has not been run again, for no change
        assert read("A") == "1"

        ppg.new()

        def do_write2(of):
            counter("A")
            append(of, "sha")

        ppg.FileGeneratingJob(of, do_write2, depend_on_function=False)
        ppg.run()
        assert read(of) == "sha"  # has been run again - number of invariants changed!
        assert read("A") == "2"

        ppg.new()
        ppg.FileGeneratingJob(of, do_write2)
        ppg.run()
        assert read(of) == "sha"  # Readding the invariant does trigger again
        assert read("A") == "3"

    def test_parameter_dependency(self):
        of = "out/a"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant("myparam", (1, 2, 3))
        job.depends_on(param_dep)
        ppg.run()
        assert read(of) == "shu"
        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant("myparam", (1, 2, 3))
        job.depends_on(param_dep)
        ppg.run()
        assert read(of) == "shu"  # has not been run again...
        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant("myparam", (1, 2, 3, 4))
        job.depends_on(param_dep)
        ppg.run()
        assert read(of) == "shushu"  # has been run again ;).

    def test_parameter_invariant_adds_hidden_job_id_prefix(self):
        param = "A"
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", param))
        jobB = ppg.ParameterInvariant("out/A", param)
        jobA.depends_on(jobB)
        ppg.run()
        assert read("out/A") == param

    def test_depends_on_func(self):
        a = ppg.FileGeneratingJob("out/A", lambda of: write("a"))
        b, a_again = a.depends_on_func("a123", lambda: 123)
        assert b.job_id.startswith("FI" + a.job_id + "_")
        assert ppg.global_pipegraph.has_edge(b, a)
        assert a_again is a

    def test_depends_on_file(self):
        a = ppg.FileGeneratingJob("out/A", lambda of: write("a"))
        write("shu", "hello")
        b = a.depends_on_file("shu")
        assert b.self is a
        assert ppg.global_pipegraph.has_edge(b.invariant, a)

    def test_depends_on_params(self):
        a = ppg.FileGeneratingJob("out/A", lambda of: write("a"))
        b = a.depends_on_params(23)
        assert b.invariant.job_id == "PIout/A"
        # assert b.invariant.parameters == 23
        assert ppg.global_pipegraph.has_edge(b.invariant, a)
        assert b.self is a

    def test_parameter_invariant_twice_different_values(self):
        ppg.ParameterInvariant("a", (1, 2, 3))
        with pytest.raises(ValueError):
            ppg.ParameterInvariant("a", (1, 2, 4))

    def test_filetime_dependency(self):
        of = "out/a"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        write(of, "hello")
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileInvariant was not stored before...
        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shu"  # job does not get rerun...

        time.sleep(1)  # so linux actually advances the file time in the next line
        write(ftfn, "hello")  # same content, different time

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shu"
        )  # job does not get rerun - filetime invariant is now filechecksum invariant...

    def test_file_did_not_exist(self):
        ppg.FileInvariant("shu")
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert "did not exist" in str(ppg.global_pipegraph.last_run_result["shu"].error)

    def test_filechecksum_dependency_raises_on_too_short_a_filename(self):
        ppg.global_pipegraph.allow_short_filenames = False
        with pytest.raises(ValueError):
            ppg.FileInvariant("a")

        with pytest.raises(ValueError):
            ppg.FileInvariant("sh")
        ppg.FileInvariant("shu")

    def test_filechecksum_dependency(self):
        of = "out/a"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        # import stat
        # logging.info('file time after creating %s'% os.stat(ftfn)[stat.ST_MTIME])

        write(of, "hello")

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileInvariant was not stored before...
        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shu"  # job does not get rerun...

        time.sleep(1)  # so linux actually advances the file time in the next line
        # logging.info("NOW REWRITE")
        write(ftfn, "hello")  # same content, different time

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shu"  # job does not get rerun...

        # time.sleep(1) #we don't care about the time, size should be enough...
        write(ftfn, "hello world!!")  # different time
        time.sleep(1)  # give the file system a second to realize the change.

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shushu"  # job does get rerun

    @pytest.mark.skip  # TODO: Renaming support?
    def test_input_file_was_renamed(self):
        of = "out/B"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        # import stat
        # logging.info('file time after creating %s'% os.stat(ftfn)[stat.ST_MTIME])

        write(of, "hello")

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileInvariant was not stored before...

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shu"  # job does not get rerun...

        os.mkdir("out/moved_here")
        shutil.move(ftfn, os.path.join("out/moved_here", "ftdep"))
        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(os.path.join("out/moved_here", "ftdep"))
        job.depends_on(dep)
        assert read(of) == "shu"  # job does not get rerun...
        ppg.run()
        assert read(of) == "shu"  # job does not get rerun...

    @pytest.mark.skip()  # I have no idea why this was useful. Possibly the PrebuildJobs support?
    def test_file_invariant_with_md5sum(self):
        of = "out/a"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        # import stat
        # logging.info('file time after creating %s'% os.stat(ftfn)[stat.ST_MTIME])

        write(of, "hello")

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileInvariant was not stored before...

        with open(ftfn + ".md5sum", "wb") as op:
            op.write(hashlib.md5(b"hello world").hexdigest().encode("utf-8"))
        write(ftfn, "hello world")  # different content
        t = time.time()
        # now make
        os.utime(ftfn, (t, t))
        os.utime(ftfn + ".md5sum", (t, t))
        time.sleep(1)  # give the file system a second to realize the change.

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shushu"
        )  # job get's run though there is a file, because the md5sum changed.

        with open(ftfn + ".md5sum", "wb") as op:
            op.write(hashlib.md5(b"hello world").hexdigest().encode("utf-8"))
        write(ftfn, "hello")  # different content, but the md5sum is stil the same!
        t = time.time()
        # now make
        os.utime(ftfn, (t, t))
        os.utime(ftfn + ".md5sum", (t, t))
        time.sleep(1)  # give the file system a second to realize the change.

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shushu"  # job does not get rerun, md5sum did not change...

        t = time.time() - 100  # force a file time mismatch
        os.utime(
            ftfn, (t, t)
        )  # I must change the one on the actual file, otherwise the 'size+filetime is the same' optimization bytes me

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shushushu"
        )  # job does get rerun, md5sum and file time mismatch
        assert os.stat(ftfn)[stat.ST_MTIME] == os.stat(ftfn + ".md5sum")[stat.ST_MTIME]

    def test_invariant_dumping_on_job_failure(self):
        def w(of):
            write("out/A", "A")
            append("out/B", "B")

        def func_c(of):
            append("out/C", "C")

        func_dep = ppg.FunctionInvariant("func_c", func_c)
        fg = ppg.FileGeneratingJob("out/A", w, depend_on_function=False)
        fg.depends_on(func_dep)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        ppg.new()

        def func_c1(of):
            append("out/C", "D")

        def w2(of):
            raise ValueError()  # so there is an error in a job...

        func_dep = ppg.FunctionInvariant("func_c", func_c1)  # so this invariant changes
        fg = ppg.FileGeneratingJob(
            "out/A", w2, depend_on_function=False
        )  # and this job crashes
        fg.depends_on(func_dep)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert not (os.path.exists("out/A"))  # since it was removed, and not recreated
        assert read("out/B") == "B"
        ppg.new()
        func_dep = ppg.FunctionInvariant(
            "func_c", func_c1
        )  # no invariant change this time
        fg = ppg.FileGeneratingJob(
            "out/A", w, depend_on_function=False
        )  # but this was not done the last time...
        fg.depends_on(func_dep)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "BB"

    def test_invariant_dumping_on_graph_exception(self, mocker):
        # when an exception occurs not within a job
        # but within the pipegraph itself (e.g. when the user hit's CTRL-C
        # during history dumping
        # which we simulate here
        def w(of):
            write("out/A", "A")
            append("out/B", "B")

        def func_c(of):
            append("out/C", "C")

        func_dep = ppg.FunctionInvariant("func_c", func_c)
        fg = ppg.FileGeneratingJob("out/A", w, depend_on_function=False)
        fg.depends_on(func_dep)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"

        ppg.new(run_mode=ppg.RunMode.CONSOLE)
        # ppg.new()

        def func_c1(of):
            append("out/C", "D")

        def w2(of):
            write("out/A", "A2")
            raise ValueError()  # so there is an error in a job...

        func_dep = ppg.FunctionInvariant("func_c", func_c1)  # so this invariant changes
        fg = ppg.FileGeneratingJob(
            "out/A", w2, depend_on_function=False
        )  # and this job crashes
        fg.depends_on(func_dep)  # so a get's deleted, and rebuild
        ppg.FileGeneratingJob("out/C", lambda of: counter("out/c") and append(of, "C"))
        assert not hasattr(ppg.global_pipegraph, "last_run_result")
        ppg.global_pipegraph._test_failing_outside_of_job = True
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        print(ppg.global_pipegraph.do_raise)
        assert isinstance(ppg.global_pipegraph.do_raise[0], KeyboardInterrupt)
        assert len(ppg.global_pipegraph.do_raise) == 2
        assert hasattr(ppg.global_pipegraph, "last_run_result")
        assert os.path.exists("out/A")  # The file get's written.
        assert read("out/B") == "B"
        assert read("out/C") == "C"
        assert read("out/c") == "1"  #
        mocker.stopall()

        ppg.new()
        func_dep = ppg.FunctionInvariant(
            "func_c", func_c1
        )  # no invariant change this time
        # but we had no stored input/output for A, right?
        # so it get's rerun
        fg = ppg.FileGeneratingJob(
            "out/A", w, depend_on_function=False
        )  # but this was not done the last time...
        fg.depends_on(func_dep)
        ppg.FileGeneratingJob("out/C", lambda of: counter("out/c") and append(of, "C"))
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "BB"
        assert read("out/C") == "C"  #
        assert read("out/c") == "1"  # c did not get rerun

    def test_sig_int_is_ignored_in_console_mode(self):
        ppg.new(run_mode=ppg.RunMode.CONSOLE)

        def sigint():
            import signal

            counter("a")
            os.kill(os.getpid(), signal.SIGINT)
            counter("A")
            return ppg.UseInputHashesForOutput()

        job = ppg.DataLoadingJob("A", sigint)
        force_load(job)
        ppg.run()
        assert read("a") == "1"
        assert read("A") == "1"

    def test_input_output_dumping_dies_for_some_reason(self, ppg2_per_test, mocker):
        ppg.global_pipegraph._test_failing_outside_of_job = True
        ppg.FileGeneratingJob("A", lambda of: counter("a") and write(of, "A"))
        ppg.FileGeneratingJob("B", lambda of: counter("b") and write(of, "B"))
        with pytest.raises(ppg.RunFailedInternally):
            ppg.run()
        assert read("A") == "A"
        assert read("B") == "B"
        assert read("a") == "1"
        assert read("b") == "1"
        ppg.run()
        assert read("A") == "A"
        assert read("B") == "B"
        assert (
            read("a") == "1"
        )  # we had captured the output for both. history saving now is all or nothing...
        assert read("b") == "1"

    def test_FileInvariant_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        write("out/shu", "shu")
        job = ppg.FileInvariant("out/shu")
        jobB = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "a"))

        with pytest.raises(ppg.JobContractError):
            job.depends_on(jobB)

    def test_FunctionInvariant_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        job = ppg.FunctionInvariant("shu", lambda: 55)
        jobB = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "a"))

        with pytest.raises(ppg.JobContractError):
            job.depends_on(jobB)

    def test_ParameterInvariant_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        job = ppg.ParameterInvariant("out/shu", ("123",))
        jobB = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "a"))

        with pytest.raises(ppg.JobContractError):
            job.depends_on(jobB)

    def test_invariant_loading_issues_on_value_catastrophic(self):
        a = ppg.DataLoadingJob("a", lambda: 5)
        b = ppg.FileGeneratingJob(
            "out/b", lambda of: write("out/b", "b"), depend_on_function=False
        )
        b.depends_on(a)
        write("out/b", "a")
        import pickle

        Path(ppg.global_pipegraph.get_history_filename()).parent.mkdir(
            parents=True, exist_ok=True
        )
        with gzip.GzipFile(ppg.global_pipegraph.get_history_filename(), "wb") as op:
            pickle.dump(a.job_id, op, pickle.HIGHEST_PROTOCOL)
            op.write(b"This breaks")
        with pytest.raises(ppg.HistoryLoadingFailed):
            ppg.run()
        assert read("out/b") == "a"  # job was not run

    def test_file_invariant_swapping(self, ppg2_per_test):
        Path("a").write_text("a")
        Path("b").write_text("b")

        def out(of):
            counter("counter")
            of.write_text(Path("a").read_text() + Path("b").read_text()),

        job = ppg.FileGeneratingJob("c", out, depend_on_function=False)
        job.depends_on_file("a")
        job.depends_on_file("b")
        ppg.run()
        assert read("c") == "ab"
        assert read("counter") == "1"
        ppg.run()
        assert read("counter") == "1"
        ppg2_per_test.new()
        job = ppg.FileGeneratingJob("c", out, depend_on_function=False)
        job.depends_on_file("b")
        job.depends_on_file("a")
        ppg.run()
        assert read("counter") == "1"

    def test_file_invariant_replaced(self):
        Path("a.tsv").write_text("a")
        a = ppg.FileInvariant("a.tsv")

        def func(of):
            counter("J")
            of.write_text("j")

        j = ppg.FileGeneratingJob("j", func)
        j.depends_on(a)
        ppg.run()
        assert read("j") == "j"
        assert read("J") == "1"
        ppg.run()
        assert read("J") == "1"
        ppg.new()
        Path("b.tsv").write_text("b")
        b = ppg.FileInvariant("b.tsv")
        j = ppg.FileGeneratingJob("j", func)
        j.depends_on(b)
        ppg.run()
        assert read("J") == "2"


def first_value(d):
    return list(d.values())[0]


@pytest.mark.usefixtures("ppg2_per_test")
class TestFunctionInvariant:
    # most of the function invariant testing is handled by other test classes.
    # but these are more specialized.

    def test_generator_expressions(self):
        def get_func(r):
            def shu():
                return sum(i + 0 for i in r)

            return shu

        def get_func2(r):
            def shu():
                return sum(i + 0 for i in r)

            return shu

        def get_func3(r):
            def shu():
                return sum(i + 1 for i in r)

            return shu

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(bv)
        )
        assert not ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(cv)
        )

    def test_lambdas(self):
        def get_func(x):
            def inner():
                arg = lambda y: x + x + x  # noqa:E731
                return arg(1)

            return inner

        def get_func2(x):
            def inner():
                arg = lambda y: x + x + x  # noqa:E731
                return arg(1)

            return inner

        def get_func3(x):
            def inner():
                arg = lambda y: x + x  # noqa:E731
                return arg(1)

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        self.maxDiff = 20000
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(bv)
        )
        assert not ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(cv)
        )

    def test_inner_functions(self):
        def get_func(x):
            def inner():
                return 23

            return inner

        def get_func2(x):
            def inner():
                return 23

            return inner

        def get_func3(x):
            def inner():
                return 23 + 5

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(bv)
        )
        assert not ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(cv)
        )

    def test_nested_inner_functions(self):
        def get_func(x):
            def inner():
                def shu():
                    return 23

                return shu

            return inner

        def get_func2(x):
            def inner():
                def shu():
                    return 23

                return shu

            return inner

        def get_func3(x):
            def inner():
                def shu():
                    return 23 + 5

                return shu

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(bv)
        )
        assert not ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(cv)
        )

    def test_inner_functions_with_parameters(self):
        def get_func(x):
            def inner():
                return x

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func(2000)
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(bv)
        )
        assert not ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(cv)
        )

    def test_passing_non_function_raises(self):
        with pytest.raises(TypeError):
            ppg.FunctionInvariant("out/a", "shu")

    def test_passing_none_as_function_is_ok(self, create_out_dir):
        job = ppg.FunctionInvariant("out/a", None)
        str(job)
        jobB = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB.depends_on(job)
        ppg.run()
        assert read("out/A") == "A"

    def test_passing_non_string_as_jobid(self):
        with pytest.raises(TypeError):
            ppg.FunctionInvariant(5, lambda: 1)

    def test_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        def shu():
            pass

        job = ppg.FunctionInvariant("shu", shu)
        jobB = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "a"))

        with pytest.raises(ppg.JobContractError):
            job.depends_on(jobB)

    def test_raises_on_duplicate_with_different_functions(self):
        def shu():
            return "a"

        ppg.FunctionInvariant("A", shu)
        ppg.FunctionInvariant("A", shu)  # ok.
        with pytest.raises(ppg.JobRedefinitionError):
            ppg.FunctionInvariant("A", lambda: "b")  # raises ValueError

        def sha():
            def shu():
                return "b"

            return shu

        ppg.FunctionInvariant("B", sha())
        ppg.FunctionInvariant("B", sha())

    def test_instance_functions_ok(self, create_out_dir):
        class shu:
            def __init__(self, letter):
                self.letter = letter

            def get_job(self):
                job = ppg.FileGeneratingJob(
                    "out/" + self.letter, lambda of: append(of, "A")
                )
                job.depends_on(ppg.FunctionInvariant("shu.sha", self.sha))
                return job

            def sha(self):
                return 55 * 23

        x = shu("A")
        x.get_job()
        ppg.run()
        assert read("out/A") == "A"
        append("out/A", "A")

        ppg.new()
        x.get_job()
        y = shu("B")
        j1 = y.get_job()
        j2 = y.get_job()
        assert ppg.FunctionInvariant.functions_equal(
            j1.generating_function, j2.generating_function
        )
        # assert j1 is j2 # todo: interactive/notebook differences

    def test_buildin_function(self):
        a = ppg.FunctionInvariant("a", open)
        assert "<built-in" in str(a)

    def test_function_invariant_non_function(self):
        class CallMe:
            def __call__(self):
                raise ValueError()

        a = ppg.FunctionInvariant("a", CallMe)
        with pytest.raises(
            ValueError
        ):  # todo: is this the right behaviour? can't we just forward to __call__ as the invariant?
            a.run(None, None)

    def test_closure_capturing(self):
        def func(da_list):
            def f():
                return da_list

            return f

        a = ppg.FunctionInvariant("a", func([1, 2, 3]))
        b = ppg.FunctionInvariant(
            "b", func([1, 2, 3])
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func([1, 2, 3, 4])
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(bv)
        )
        assert not ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(cv)
        )

    def test_function_to_str_builtin(self):
        assert ppg.FunctionInvariant.function_to_str(open) == "<built-in function open>"

    def test_closure_capturing_dict(self):
        def func(da_list):
            def f():
                return da_list

            return f

        a = ppg.FunctionInvariant("a", func({"1": "a", "3": "b", "2": "c"}))
        b = ppg.FunctionInvariant(
            "b", func({"1": "a", "3": "b", "2": "c"})
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func({"1": "a", "3": "b", "2": "d"})
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(bv)
        )
        assert not ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(cv)
        )

    def test_closure_capturing_set(self):
        def func(da_list):
            def f():
                return da_list

            return f

        import random

        x = set(["1", "2", "3", "4", "5", "6", "7", "8"])
        a = ppg.FunctionInvariant("a", func(x))
        x2 = list(x)
        random.shuffle(x2)
        x2 = set(x2)
        b = ppg.FunctionInvariant("b", func(x2))  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func({"3", "2"})
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(bv)
        )
        assert not ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(cv)
        )

    def test_closure_capturing_frozen_set(self):
        def func(da_list):
            def f():
                return da_list

            return f

        import random

        x = frozenset(["1", "2", "3", "4", "5", "6", "7", "8"])
        a = ppg.FunctionInvariant("a", func(x))
        x2 = list(x)
        random.shuffle(x2)
        x2 = frozenset(x2)
        b = ppg.FunctionInvariant("b", func(x2))  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func(frozenset({"3", "2"}))
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(bv)
        )
        assert not ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(cv)
        )

    def test_function_invariants_are_equal_if_dis_identical_or_source_identical(self):
        python_version = (3, 99)
        a = {"source": "hello", python_version: ("dis", "closure")}
        b = a.copy()
        b["source"] = "hello_world"
        c = a.copy()
        c[python_version] = ("disB", "closure")
        assert ppg.FunctionInvariant.compare_hashes(None, a, a, python_version)
        assert ppg.FunctionInvariant.compare_hashes(
            None, a, b, python_version
        )  # same dis ,different source
        assert not ppg.FunctionInvariant.compare_hashes(
            None, a, c, python_version
        )  # different dis, same source

    def test_source_file_mtime_change_without_hash_change(self):
        def inner():
            pass

        # python_version = tuple(sys.version_info)[:2]  # we only care about major.minor

        a = ppg.FunctionInvariant("a", inner)
        calc = a.run(None, None)
        changed = calc.copy()
        changed["FIa"]["source_file"]["mtime"] = -1
        changed["FIa"]["source_file"]["dis"] = "find me"
        calc2 = a.run(None, changed)
        assert calc2["FIa"]["source_file"]["dis"] == "find me"


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg2_per_test")
class TestDependency:
    def test_simple_chain(self):
        o = Dummy()

        def load_a():
            return "shu"

        jobA = ppg.AttributeLoadingJob("a", o, "myattr", load_a)
        ofB = "out/B"

        def do_write_b(ofB):
            write(ofB, o.myattr)

        jobB = ppg.FileGeneratingJob(ofB, do_write_b).depends_on(jobA)
        ofC = "out/C"

        def do_write_C(ofC):
            write(ofC, o.myattr)

        ppg.FileGeneratingJob(ofC, do_write_C).depends_on(jobA)

        ofD = "out/D"

        def do_write_d(ofD):
            write(ofD, read(ofC) + read(ofB))

        ppg.FileGeneratingJob(ofD, do_write_d).depends_on([jobA, jobB])

    def test_failed_job_kills_those_after(self):
        ofA = "out/A"

        def write_a(ofA):
            append(ofA, "hello")

        jobA = ppg.FileGeneratingJob(ofA, write_a)

        ofB = "out/B"

        def write_b(ofB):
            raise ValueError("shu")

        jobB = ppg.FileGeneratingJob(ofB, write_b)
        jobB.depends_on(jobA)

        ofC = "out/C"

        def write_c(ofC):
            write(ofC, "hello")

        jobC = ppg.FileGeneratingJob(ofC, write_c)
        jobC.depends_on(jobB)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert os.path.exists(ofA)  # which was before the error
        assert not (os.path.exists(ofB))  # which was on the error
        assert not (os.path.exists(ofC))  # which was after the error
        ppg.new()
        jobA = ppg.FileGeneratingJob(ofA, write_a)
        jobC = ppg.FileGeneratingJob(ofC, write_c)

        def write_b_ok(ofB):
            write(ofB, "BB")

        jobB = ppg.FileGeneratingJob(ofB, write_b_ok)
        jobB.depends_on(jobA)
        jobC.depends_on(jobB)
        ppg.run()

        assert os.path.exists(ofA)
        assert read(ofA) == "hello"  # run only once!
        assert os.path.exists(ofB)
        assert os.path.exists(ofC)

    def test_done_filejob_does_not_gum_up_execution(self):
        ofA = "out/A"
        write(ofA, "1111")

        def write_a(ofA):
            append(ofA, "hello")

        jobA = ppg.FileGeneratingJob(ofA, write_a, depend_on_function=False)

        ofB = "out/B"

        def write_b(ofB):
            append(ofB, "hello")

        jobB = ppg.FileGeneratingJob(ofB, write_b)
        jobB.depends_on(jobA)

        ofC = "out/C"

        def write_c(ofC):
            write(ofC, "hello")

        jobC = ppg.FileGeneratingJob(ofC, write_c)
        jobC.depends_on(jobB)
        assert os.path.exists(ofA)

        ppg.run()

        assert os.path.exists(ofB)
        assert os.path.exists(ofC)
        assert (
            read(ofA) == "hello"
        )  # change from ppgA, if it's not our file (we have recorded not output), rerun

    def test_invariant_violation_redoes_deps_but_not_nondeps(self):
        def get_job(name):
            fn = "out/" + name

            def do_write(of):
                if os.path.exists(fn + ".sentinel"):
                    d = read(fn + ".sentinel")
                else:
                    d = ""
                append(fn + ".sentinel", name)  # get's longer all the time...
                write(fn, d + name)  # get's deleted anyhow...

            return ppg.FileGeneratingJob(fn, do_write)

        jobA = get_job("A")
        jobB = get_job("B")
        jobC = get_job("C")
        get_job("D")
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        dep = ppg.ParameterInvariant("myparam", ("hello",))
        jobA.depends_on(dep)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

        ppg.new()
        jobA = get_job("A")
        jobB = get_job("B")
        jobC = get_job("C")
        get_job("D")
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        dep = ppg.ParameterInvariant("myparam", ("hello stranger",))
        jobA.depends_on(dep)  # now, the invariant has been changed, all jobs rerun...
        ppg.run()
        assert read("out/A") == "AA"  # thanks to our smart rerun aware job definition..
        assert read("out/B") == "BB"
        assert read("out/C") == "CC"
        assert read("out/D") == "D"  # since that one does not to be rerun...

    def test_depends_on_accepts_a_list(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))
        jobC.depends_on([jobA, jobB])
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_job_iter(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        ll = list(iter(jobA))
        assert ll[0] is jobA

    def test_depends_on_accepts_multiple_values(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))
        jobC.depends_on(jobA, jobB)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_depends_on_accepts_multiple_values_mixed(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))
        jobC.depends_on(jobA, [jobB])
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_depends_on_none_ignored(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))
        jobC.depends_on(jobA, [jobB], None, [None])
        jobC.depends_on(None)
        jobC.depends_on()  # that's a no-op as well
        jobC.depends_on([])  # that's a no-op as well
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_depends_on_excludes_on_non_jobs(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))

        with pytest.raises(KeyError):
            jobA.depends_on("SHU")

    def test_depends_on_instant_cycle_check(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/b", lambda of: write("out/B", "b"))
        jobB.depends_on(jobA)

        with pytest.raises(ppg.NotADag):
            jobA.depends_on(jobA)

        with pytest.raises(ppg.NotADag):
            jobA.depends_on(jobB)

    def test_depends_on_accepts_a_list_of_lists(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob(
            "out/C",
            lambda of: write("out/C", read("out/A") + read("out/B") + read("out/D")),
        )
        jobD = ppg.FileGeneratingJob("out/D", lambda of: write("out/D", "D"))
        jobC.depends_on([jobA, [jobB, jobD]])
        assert ppg.global_pipegraph.has_edge(jobD, jobC)
        assert ppg.global_pipegraph.has_edge(jobA, jobC)
        assert ppg.global_pipegraph.has_edge(jobB, jobC)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "ABD"
        assert read("out/D") == "D"

    def test_invariant_job_depends_on_raises(self):
        with pytest.raises(ppg.JobContractError):
            ppg.jobs._InvariantMixin().depends_on(ppg.Job(["B"]))

    def test_cached_job_depends_on(self):
        class Dummy:
            pass

        o = Dummy()
        jobA = ppg.CachedAttributeLoadingJob("cache/A", o, "a", lambda: 23)
        jobB = ppg.Job(["B"])
        jobC = ppg.Job(["C"])
        jobD = ppg.Job(["D"])
        jobA.calc.depends_on([jobB], jobC, jobD)
        assert not ppg.global_pipegraph.has_edge(jobB, jobA.load)
        assert not ppg.global_pipegraph.has_edge(jobC, jobA.load)
        assert not ppg.global_pipegraph.has_edge(jobD, jobA.load)
        assert ppg.global_pipegraph.has_edge(jobB, jobA.calc)
        assert ppg.global_pipegraph.has_edge(jobC, jobA.calc)
        assert ppg.global_pipegraph.has_edge(jobD, jobA.calc)

    def test_dependency_placeholder(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda of: write("out/A", "A" + read("out/B"))
        )
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))

        def gen_deps():
            logger.info("gen deps called")
            return [jobB]

        jobA.depends_on(gen_deps)
        ppg.run()
        assert read("out/A") == "AB"

    def test_dependency_placeholder2(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda of: write("out/A", "A" + read("out/B"))
        )

        def gen_deps():
            return ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))

        jobA.depends_on(gen_deps)
        ppg.run()
        assert read("out/A") == "AB"

    def test_dependency_placeholder_nested(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda of: write("out/A", "A" + read("out/B") + read("out/C"))
        )

        def gen_deps2():
            return ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))

        def gen_deps():
            return ppg.FileGeneratingJob(
                "out/B", lambda of: write("out/B", "B")
            ).depends_on(gen_deps2)

        jobA.depends_on(gen_deps)
        ppg.run()
        assert read("out/A") == "ABC"

    def test_dependency_placeholder_dynamic_auto_invariants(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda of: write("out/A", "A" + read("out/B"))
        )

        def check_function_invariant(of):
            write("out/B", "B")
            assert (
                "FITestDependency.test_dependency_placeholder_dynamic_auto_invariants.<locals>.check_function_invariant"
                in ppg.global_pipegraph.jobs
            )

        def gen_deps():
            jobB = ppg.FileGeneratingJob("out/B", check_function_invariant)
            print("gen deps called")
            return [jobB]

        jobA.depends_on(gen_deps)
        assert "FIout/B" not in ppg.global_pipegraph.jobs
        ppg.run()
        assert read("out/A") == "AB"


@pytest.mark.usefixtures("ppg2_per_test")
class TestDefinitionErrors:
    def test_defining_function_invariant_twice(self):
        a = lambda: 55  # noqa:E731
        b = lambda: 66  # noqa:E731
        ppg.FunctionInvariant("a", a)
        ppg.FunctionInvariant("a", a)  # that's ok...

        with pytest.raises(ppg.JobRedefinitionError):
            ppg.FunctionInvariant("a", b)

        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        ppg.FunctionInvariant("a", a)
        j = ppg.FunctionInvariant("a", b)
        assert j.function is b

    def test_defining_function_and_parameter_invariant_with_same_name(self):
        # you can't really, FunctionInvariants are Prefixed with FI, ParameterInvariant with PI
        a = lambda: 55  # noqa:E731
        ppg.FunctionInvariant("PIa", a)
        ppg.ParameterInvariant("a", "b")

    def test_defining_function_and_parameter_invariant_with_same_name_reversed(self):
        a = lambda: 55  # noqa:E731
        ppg.ParameterInvariant("a", "b")
        ppg.FunctionInvariant("PIa", a)

    def test_parameter_invariant_does_not_accept_function(self):
        with pytest.raises(TypeError):
            ppg.ParameterInvariant("a", lambda: 55)


@pytest.mark.usefixtures("ppg2_per_test")
class TestFunctionInvariantDisChanges_BetweenVersions:
    def test_function_name_is_irrelevant(self):
        def test_a():
            return 55

        def test_b():
            return 55

        def test_c():
            return 56

        a = ppg.FunctionInvariant("a", test_a)
        b = ppg.FunctionInvariant("b", test_b)
        c = ppg.FunctionInvariant("c", test_c)
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(bv)
        )
        assert not ppg.FunctionInvariant.compare_hashes(
            None, first_value(av), first_value(cv)
        )

    def test_docstring_is_irrelevant(self):
        def test():
            """A"""
            return 55

        a = ppg.FunctionInvariant("a", test)

        # fmt: off
        def test():
            '''B'''
            return 55
        # fmt: on
        b = ppg.FunctionInvariant("b", test)

        def test():
            "c"
            return 56

        c = ppg.FunctionInvariant("c", test)

        def test():
            "c"
            return 56

        d = ppg.FunctionInvariant("d", test)
        av = first_value(a.run(None, None))
        bv = first_value(b.run(None, None))
        cv = first_value(c.run(None, None))
        dv = first_value(d.run(None, None))
        assert ppg.FunctionInvariant.compare_hashes(None, (av), (bv))
        assert ppg.FunctionInvariant.compare_hashes(None, (cv), (dv))
        assert not ppg.FunctionInvariant.compare_hashes(None, (av), (cv))
